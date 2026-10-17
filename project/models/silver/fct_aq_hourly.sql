{{ config(
    materialized = 'incremental',
    unique_key = ['station_key', 'hour_start_utc'],
    incremental_strategy = 'merge'
) }}

WITH bounds AS (
    SELECT MAX(period_start_utc) AS max_hour FROM {{ ref('stg_aq') }}
),

grid AS (
    SELECT
        h.hour_start_utc,
        h.hour_start_ist,
        h.date_ist,
        h.season,
        s.station_key,
        s.zone,
        s.is_forecast_target
    FROM {{ ref('dim_hour') }} AS h
    CROSS JOIN {{ ref('dim_station') }} AS s
    CROSS JOIN bounds AS b
    WHERE h.is_burning_season
      AND h.date_ist >= s.available_from
      AND h.hour_start_utc <= b.max_hour
    {% if is_incremental() %}
      AND h.hour_start_utc >= (SELECT DATEADD(day, -8, MAX(hour_start_utc)) FROM {{ this }})
    {% endif %}
),

joined AS (
    SELECT
        g.*,
        a.pm25,
        a.percent_coverage,
        a.observed_count,
        a.expected_count,
        a.has_flags
    FROM grid AS g
    LEFT JOIN {{ ref('stg_aq') }} AS a
      ON a.station_key = g.station_key
     AND a.period_start_utc = g.hour_start_utc
),

windowed AS (
    SELECT
        *,
        LAG(pm25)   OVER (PARTITION BY station_key ORDER BY hour_start_utc)                                            AS prev_pm25,
        COUNT(pm25) OVER (PARTITION BY station_key ORDER BY hour_start_utc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS n_last6,
        MIN(pm25)   OVER (PARTITION BY station_key ORDER BY hour_start_utc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS min_last6,
        MAX(pm25)   OVER (PARTITION BY station_key ORDER BY hour_start_utc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS max_last6
    FROM joined
),

stuck AS (
    SELECT
        *,
        IFF(n_last6 = 6 AND min_last6 = max_last6, 1, 0) AS ends_stuck_run
    FROM windowed
),

flagged AS (
    SELECT
        *,
        MAX(ends_stuck_run) OVER (PARTITION BY station_key ORDER BY hour_start_utc
                                  ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING) = 1   AS is_stuck_sensor,
        (pm25 IS NULL)                                                            AS is_gap,
        COALESCE(percent_coverage < 75, FALSE)                                    AS is_low_coverage,
        COALESCE(pm25 > 3 * prev_pm25 AND pm25 - prev_pm25 > 100, FALSE)          AS is_spike
    FROM stuck
)

SELECT
    station_key,
    hour_start_utc,
    hour_start_ist,
    date_ist,
    season,
    zone,
    is_forecast_target,
    pm25,
    percent_coverage,
    observed_count,
    expected_count,
    has_flags,
    is_gap,
    is_low_coverage,
    is_stuck_sensor,
    is_spike,
    (NOT is_gap AND NOT is_low_coverage AND NOT is_stuck_sensor AND NOT is_spike) AS is_usable
FROM flagged
{% if is_incremental() %}
WHERE hour_start_utc >= (SELECT DATEADD(day, -7, MAX(hour_start_utc)) FROM {{ this }})
{% endif %}