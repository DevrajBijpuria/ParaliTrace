WITH thresholds AS (
    SELECT column1 AS source, column2 AS max_data_lag_h, column3 AS max_load_lag_h
    FROM VALUES
        ('firms', 6, 6),
        ('openaq', 6, 3),
        ('openmeteo', 3, 3)
),

data_latest AS (
    SELECT 'firms' AS source, MAX(hour_start_utc) AS latest_event_utc
    FROM {{ ref('fct_fire_detection') }}

    UNION ALL

    SELECT 'openaq' AS source, MAX(hour_start_utc) AS latest_event_utc
    FROM {{ ref('fct_aq_hourly') }}
    WHERE pm25 IS NOT NULL

    UNION ALL

    SELECT 'openmeteo' AS source, MAX(hour_start_utc) AS latest_event_utc
    FROM {{ ref('fct_weather_hourly') }}
    WHERE NOT is_gap
),

load_latest AS (
    SELECT source, MAX(last_loaded_at) AS latest_load_at
    FROM {{ ref('load_audit') }}
    GROUP BY source
)

SELECT
    t.source,
    d.latest_event_utc,
    l.latest_load_at,
    DATEDIFF(HOUR, d.latest_event_utc, SYSDATE()) AS data_lag_h,
    DATEDIFF(HOUR, l.latest_load_at, CURRENT_TIMESTAMP()) AS load_lag_h,
    t.max_data_lag_h,
    t.max_load_lag_h,
    CASE
        WHEN d.latest_event_utc IS NULL THEN 'NO_DATA'
        WHEN DATEDIFF(HOUR, d.latest_event_utc, SYSDATE()) > t.max_data_lag_h * 4 THEN 'STALE'
        WHEN DATEDIFF(HOUR, d.latest_event_utc, SYSDATE()) > t.max_data_lag_h THEN 'WARN'
        ELSE 'OK'
    END AS freshness_status,
    SYSDATE() AS checked_at_utc
FROM thresholds t
LEFT JOIN data_latest d ON d.source = t.source
LEFT JOIN load_latest l ON l.source = t.source