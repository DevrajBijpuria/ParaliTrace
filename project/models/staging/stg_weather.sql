WITH source as (
    SELECT * FROM {{source('raw','WEATHER_BRONZE')}}
),
deduped as(
    SELECT * ,
    ROW_NUMBER()OVER(PARTITION BY point_key, ts_utc, source ORDER BY _loaded_at DESC) AS rn
    FROM source
)

SELECT
    md5(CONCAT_WS('|', point_key, ts_utc, source)) AS weather_reading_id,
    point_key,
    ts_utc,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', ts_utc)::TIMESTAMP_NTZ  AS ts_ist,
    TO_DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', ts_utc)) AS date_ist,
    requested_lat,
    requested_lon,
    grid_lat,
    grid_lon,
    elevation,
    wind_speed_10m,
    wind_direction_10m,
    wind_speed_100m,
    wind_direction_100m,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    boundary_layer_height,
    source,                                                                 
    ingested_at,
    _loaded_at
    FROM deduped WHERE rn =1