{{ config(materialized='table') }}

with delhi as (
    select lat as delhi_lat, lon as delhi_lon
    from {{ ref('dim_route_point') }}
    where point_key = 'delhi'
),

upwind_points as (
    select point_key, lat, lon
    from {{ ref('dim_route_point') }}
    where point_key <> 'delhi'
),
weather as (
    select
        point_key,
        hour_start_utc,
        avg(transport_alignment) over (
            partition by point_key order by hour_start_utc
            rows between current row and 5 following
        ) as align_6h,
        avg(transport_speed_ms) over (
            partition by point_key order by hour_start_utc
            rows between current row and 5 following
        ) as tspeed_6h,
        avg(wind_speed_100m) over (
            partition by point_key order by hour_start_utc
            rows between current row and 5 following
        ) as ws_6h
    from {{ ref('fct_weather_hourly') }}
),

fires as (
    select
        f.fire_id,
        f.hour_start_utc,
        f.cell_lat,
        f.cell_lon,
        f.frp_mw,
        f.region_key,
        haversine(f.cell_lat, f.cell_lon, d.delhi_lat, d.delhi_lon) as dist_to_delhi_km
    from {{ ref('fct_fire_detection') }} f
    cross join delhi d
    where f.region_key in ('punjab', 'haryana')
      and not f.is_low_confidence
),

nearest as (
    select
        fi.*,
        p.point_key
    from fires fi
    cross join upwind_points p
    qualify row_number() over (
        partition by fi.fire_id
        order by haversine(fi.cell_lat, fi.cell_lon, p.lat, p.lon)
    ) = 1
),

transport as (
    select
        n.*,
        w.align_6h,
        w.tspeed_6h,
        w.ws_6h,
        n.dist_to_delhi_km * 1000 / nullif(w.tspeed_6h, 0) / 3600 as travel_hours_raw
    from nearest n
    left join weather w
        on  w.point_key = n.point_key
        and w.hour_start_utc = n.hour_start_utc
),
arrivals as (
    select
        fire_id,
        region_key,
        frp_mw,
        hour_start_utc as fire_hour_utc,
        case when ws_6h < 2 then 48
             else greatest(round(travel_hours_raw), 1)
        end as travel_hours,
        dateadd(hour,
                case when ws_6h < 2 then 48
                     else greatest(round(travel_hours_raw), 1)
                end,
                hour_start_utc) as arrival_hour_utc,
        frp_mw
          * case when ws_6h < 2 then 0.5 else align_6h end
          * exp(-dist_to_delhi_km / {{ var('smoke_decay_km', 300) }}) as smoke_weight
    from transport
    where ws_6h < 2
       or (align_6h >= 0.5 and travel_hours_raw <= 72)
)

select * from arrivals