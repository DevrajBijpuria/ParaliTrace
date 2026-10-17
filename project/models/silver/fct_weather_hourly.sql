{{ config(
    materialized = 'incremental',
    unique_key = ['point_key', 'hour_start_utc'],
    incremental_strategy = 'merge'
) }}
with bounds as (
    select max(ts_utc) as max_hour
    from {{ ref('stg_weather') }}
),

best_reading as (
    select *
    from {{ ref('stg_weather') }}
    qualify row_number() over (
        partition by point_key, ts_utc
        order by case source when 'open_meteo_archive' then 1 else 2 end, _loaded_at desc
    ) = 1
),

grid as (
    select
        h.hour_start_utc,
        h.hour_start_ist,
        h.date_ist,
        h.season,
        h.is_night_ist,
        p.point_key,
        p.segment,
        p.route_order,
        p.is_receptor,
        p.distance_to_delhi_km,
        p.transport_wind_from_deg
    from {{ ref('dim_hour') }} h
    cross join {{ ref('dim_route_point') }} p
    cross join bounds b
    where h.is_burning_season
      and h.hour_start_utc <= b.max_hour
    {% if is_incremental() %}
      and h.hour_start_utc >= (select dateadd(day, -7, max(hour_start_utc)) from {{ this }})
    {% endif %}
)

select
    g.point_key,
    g.hour_start_utc,
    g.hour_start_ist,
    g.date_ist,
    g.season,
    g.is_night_ist,
    g.segment,
    g.route_order,
    g.is_receptor,
    g.distance_to_delhi_km,

    w.wind_speed_10m,
    w.wind_direction_10m,
    w.wind_speed_100m,
    w.wind_direction_100m,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.precipitation,
    w.boundary_layer_height,


    -w.wind_speed_100m * sin(radians(w.wind_direction_100m))                      as wind_u_100m,
    -w.wind_speed_100m * cos(radians(w.wind_direction_100m))                      as wind_v_100m,

    case when not g.is_receptor
         then cos(radians(w.wind_direction_100m - g.transport_wind_from_deg))
    end                                                                           as transport_alignment,

  
    case when not g.is_receptor
         then w.wind_speed_100m * greatest(cos(radians(w.wind_direction_100m - g.transport_wind_from_deg)), 0)
    end                                                                           as transport_speed_ms,

    case w.source when 'open_meteo_archive' then 'archive'
                  when 'open_meteo_forecast' then 'forecast' end                  as weather_source,
    (w.point_key is null)                                                         as is_gap
from grid g
left join best_reading w
  on w.point_key = g.point_key
 and w.ts_utc = g.hour_start_utc