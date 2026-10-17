WITH source as (
    SELECT * FROM {{source('raw','AQ_BRONZE')}} WHERE is_canonical_hour = TRUE
),
deduped as(
    SELECT * ,
    ROW_NUMBER()OVER(PARTITION BY sensor_id, period_start_utc ORDER BY _loaded_at DESC) AS rn
    FROM source
)

SELECT
    md5(CONCAT_WS('|', sensor_id, period_start_utc)) AS aq_reading_id,
    station_key,
    location_id,
    sensor_id,
    period_start_utc,
    period_end_utc,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', period_start_utc)::TIMESTAMP_NTZ AS period_start_ist,
    TO_DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', period_start_utc)) AS date_ist,
    pm25,
    units,
    observed_count,
    expected_count,
    percent_coverage,
    has_flags,
    source,
    ingested_at,
    _loaded_at
    FROM deduped WHERE rn =1