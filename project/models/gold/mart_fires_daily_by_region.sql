SELECT
    DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hour_start_utc)) AS date_ist,
    YEAR(hour_start_utc) AS season,
    region_key,
    COUNT(*) AS fire_events,
    SUM(n_detections) AS detections,
    ROUND(SUM(frp_mw), 1) AS total_frp_mw,
    ROUND(MAX(frp_mw), 1) AS max_frp_mw
FROM {{ ref('fct_fire_detection') }}
WHERE NOT is_low_confidence
GROUP BY 1, 2, 3