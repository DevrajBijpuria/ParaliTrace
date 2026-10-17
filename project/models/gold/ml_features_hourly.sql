with hours as (
    select hour_start_utc, date_ist, season, hour_of_day_ist, is_peak_burning, is_night_ist
    from {{ ref('dim_hour') }}
    where is_burning_season
),

aq as (
    select hour_start_utc, iff(is_usable, pm25, null) as pm25
    from {{ ref('fct_aq_hourly') }}
    where station_key = 'new_delhi_airnow'
),

delhi_wx as (
    select hour_start_utc,
           boundary_layer_height as blh,
           wind_speed_10m        as ws10,
           temperature_2m        as temp,
           relative_humidity_2m  as rh,
           precipitation         as precip
    from {{ ref('fct_weather_hourly') }}
    where point_key = 'delhi'
),

upwind_wx as (
    select hour_start_utc,
           avg(transport_alignment) as upwind_align,
           avg(wind_speed_100m)     as upwind_ws100
    from {{ ref('fct_weather_hourly') }}
    where point_key <> 'delhi'
    group by hour_start_utc
),

smoke as (
    select hour_start_utc, smoke_index_log,
           ln(1 + upwind_frp_24h_naive) as naive_log
    from {{ ref('fct_smoke_index_hourly') }}
), 
transit as (
    select
        h.hour_start_utc,
        coalesce(sum(iff(a.arrival_hour_utc <= dateadd(hour, 24, h.hour_start_utc),
                         a.smoke_weight, 0)), 0) as smoke_transit_24h,
        coalesce(sum(a.smoke_weight), 0)          as smoke_transit_48h
    from hours h
    left join {{ ref('fct_smoke_arrivals') }} a
        on  a.fire_hour_utc   <= dateadd(hour, -6, h.hour_start_utc)
        and a.arrival_hour_utc >  h.hour_start_utc
        and a.arrival_hour_utc <= dateadd(hour, 48, h.hour_start_utc)
    group by h.hour_start_utc
),

base as (
    select
        h.*,
        aq.pm25,
        w.blh, w.ws10, w.temp, w.rh, w.precip,
        w.blh * w.ws10 as ventilation,
        u.upwind_align, u.upwind_ws100,
        s.smoke_index_log, s.naive_log,
        t.smoke_transit_24h, t.smoke_transit_48h
    from hours h
    left join aq        on aq.hour_start_utc = h.hour_start_utc
    left join delhi_wx w on w.hour_start_utc = h.hour_start_utc
    left join upwind_wx u on u.hour_start_utc = h.hour_start_utc
    left join smoke s   on s.hour_start_utc  = h.hour_start_utc
    left join transit t on t.hour_start_utc  = h.hour_start_utc
)

select
    hour_start_utc as issue_hour_utc,
    date_ist,
    season,
    hour_of_day_ist,
    dayofweek(date_ist) as day_of_week,
    datediff(day, date_from_parts(season, 9, 15), date_ist) as day_of_season,
    is_peak_burning,
    is_night_ist,
    (date_ist in ('2023-11-11','2023-11-12','2023-11-13',
                  '2024-10-30','2024-10-31','2024-11-01',
                  '2025-10-19','2025-10-20','2025-10-21')) as is_diwali_window,

    -- targets (future, never use as features)
    lead(pm25, 24) over (partition by season order by hour_start_utc) as target_pm25_24h,
    lead(pm25, 48) over (partition by season order by hour_start_utc) as target_pm25_48h,

    -- PM2.5 history
    pm25                                                                  as pm25_now,
    lag(pm25, 1)  over (partition by season order by hour_start_utc)      as pm25_lag1,
    lag(pm25, 3)  over (partition by season order by hour_start_utc)      as pm25_lag3,
    lag(pm25, 6)  over (partition by season order by hour_start_utc)      as pm25_lag6,
    lag(pm25, 24) over (partition by season order by hour_start_utc)      as pm25_lag24,
    avg(pm25) over (partition by season order by hour_start_utc
                    rows between 23 preceding and current row)            as pm25_mean_24h,
    max(pm25) over (partition by season order by hour_start_utc
                    rows between 23 preceding and current row)            as pm25_max_24h,

    -- smoke (lagged: index uses wind up to ~5h after the fire; FIRMS has ~3h latency)
    lag(smoke_index_log, 6)  over (partition by season order by hour_start_utc) as smoke_index_log_lag6,
    lag(smoke_index_log, 12) over (partition by season order by hour_start_utc) as smoke_index_log_lag12,
    lag(naive_log, 3)        over (partition by season order by hour_start_utc) as naive_log_lag3,
    ln(1 + smoke_transit_24h) as smoke_transit_log_24h,
    ln(1 + smoke_transit_48h) as smoke_transit_log_48h,

    -- weather now
    blh, ws10, temp, rh, precip, ventilation,
    min(blh) over (partition by season order by hour_start_utc
                   rows between 23 preceding and current row)             as blh_min_24h,
    sum(precip) over (partition by season order by hour_start_utc
                      rows between 23 preceding and current row)          as precip_24h,
    upwind_align,
    upwind_ws100,

    -- weather at target time (backtest uses actuals as a "perfect forecast")
    lead(blh, 24)         over (partition by season order by hour_start_utc) as blh_at_24h,
    lead(ventilation, 24) over (partition by season order by hour_start_utc) as ventilation_at_24h,
    lead(blh, 48)         over (partition by season order by hour_start_utc) as blh_at_48h,
    lead(ventilation, 48) over (partition by season order by hour_start_utc) as ventilation_at_48h,

    case when season <= 2024 then 'train'
         when season = 2025  then 'test'
         else 'live' end as split

from base