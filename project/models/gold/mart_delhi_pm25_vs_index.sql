with smoke as (
    select
        hour_start_utc,
        date_ist,
        season,
        is_peak_burning,
        smoke_index,
        smoke_index_log,
        upwind_frp_24h_naive,
        lag(smoke_index_log, 12) over (partition by season order by hour_start_utc) as smoke_index_log_lag12,
        lag(smoke_index_log, 24) over (partition by season order by hour_start_utc) as smoke_index_log_lag24,
        lag(smoke_index_log, 36) over (partition by season order by hour_start_utc) as smoke_index_log_lag36,
        lag(smoke_index_log, 48) over (partition by season order by hour_start_utc) as smoke_index_log_lag48
    from {{ ref('fct_smoke_index_hourly') }}
),

delhi_wx as (
    select
        hour_start_utc,
        boundary_layer_height  as delhi_blh_m,
        temperature_2m         as delhi_temp_c,
        relative_humidity_2m   as delhi_rh_pct,
        precipitation          as delhi_precip_mm,
        wind_speed_10m         as delhi_wind_10m_ms
    from {{ ref('fct_weather_hourly') }}
    where point_key = 'delhi'
)

select
    a.station_key,
    st.station_name,
    st.zone,
    st.is_forecast_target,

    s.hour_start_utc,
    s.date_ist,
    s.season,
    s.is_peak_burning,

    a.pm25,
    a.is_usable,

    s.smoke_index,
    s.smoke_index_log,
    s.smoke_index_log_lag12,
    s.smoke_index_log_lag24,
    s.smoke_index_log_lag36,
    s.smoke_index_log_lag48,
    s.upwind_frp_24h_naive,

    w.delhi_blh_m,
    w.delhi_temp_c,
    w.delhi_rh_pct,
    w.delhi_precip_mm,
    w.delhi_wind_10m_ms

from {{ ref('fct_aq_hourly') }} a
join smoke s
    on s.hour_start_utc = a.hour_start_utc
join {{ ref('dim_station') }} st
    on st.station_key = a.station_key
left join delhi_wx w
    on w.hour_start_utc = a.hour_start_utc