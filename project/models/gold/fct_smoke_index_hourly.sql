
with arrivals as (
    select * from {{ ref('fct_smoke_arrivals') }}
),
fires as (
    select hour_start_utc, frp_mw
    from {{ ref('fct_fire_detection') }}
    where region_key in ('punjab', 'haryana')
      and not is_low_confidence
),
arrivals_hourly as (
    select
        arrival_hour_utc,
        count(*)                                          as n_fires_arriving,
        sum(frp_mw)                                       as frp_arriving_mw,
        sum(smoke_weight)                                 as smoke_arriving,
        sum(iff(region_key = 'punjab',  smoke_weight, 0)) as smoke_arriving_punjab,
        sum(iff(region_key = 'haryana', smoke_weight, 0)) as smoke_arriving_haryana,
        avg(travel_hours)                                 as avg_travel_hours
    from arrivals
    group by arrival_hour_utc
),

hours as (
    select hour_start_utc, date_ist, season, is_peak_burning
    from {{ ref('dim_hour') }}
    where is_burning_season
),
index_calc as (
    select
        h.hour_start_utc,
        coalesce(sum(
            a.smoke_arriving
            * exp(-datediff(hour, a.arrival_hour_utc, h.hour_start_utc) / {{ var('smoke_persistence_h', 12) }})
        ), 0) as smoke_index
    from hours h
    left join arrivals_hourly a
        on a.arrival_hour_utc between dateadd(hour, -23, h.hour_start_utc) and h.hour_start_utc
    group by h.hour_start_utc
),
fires_hourly as (
    select hour_start_utc, sum(frp_mw) as frp_mw, count(*) as n_fires
    from fires
    group by hour_start_utc
)

select
    h.hour_start_utc,
    h.date_ist,
    h.season,
    h.is_peak_burning,

    coalesce(a.n_fires_arriving, 0)        as n_fires_arriving,
    coalesce(a.frp_arriving_mw, 0)         as frp_arriving_mw,
    coalesce(a.smoke_arriving, 0)          as smoke_arriving,
    coalesce(a.smoke_arriving_punjab, 0)   as smoke_arriving_punjab,
    coalesce(a.smoke_arriving_haryana, 0)  as smoke_arriving_haryana,
    a.avg_travel_hours,

    i.smoke_index,
    ln(1 + i.smoke_index)                  as smoke_index_log,

    sum(coalesce(fh.frp_mw, 0)) over (
        order by h.hour_start_utc rows between 23 preceding and current row
    ) as upwind_frp_24h_naive,
    sum(coalesce(fh.n_fires, 0)) over (
        order by h.hour_start_utc rows between 23 preceding and current row
    ) as upwind_fires_24h_naive

from hours h
left join arrivals_hourly a on a.arrival_hour_utc = h.hour_start_utc
left join index_calc i      on i.hour_start_utc   = h.hour_start_utc
left join fires_hourly fh   on fh.hour_start_utc  = h.hour_start_utc