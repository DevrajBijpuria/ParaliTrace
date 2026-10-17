WITH source as (
    SELECT * FROM {{source('raw','FIRES_BRONZE')}}
),
deduped as(
    SELECT * ,
    ROW_NUMBER()OVER(PARTITION BY latitude, longitude, acq_ts_utc, satellite_std ORDER BY _loaded_at DESC) AS rn
    FROM source
)

SELECT 
   MD5(concat_ws('|', latitude, longitude, acq_ts_utc, satellite_std)) AS fire_detection_id,
   latitude,
   longitude,
   acq_ts_utc,
   CONVERT_TIMEZONE('UTC','Asia/Kolkata',acq_ts_utc) :: TIMESTAMP_NTZ AS acq_ts_ist,
   TO_DATE(CONVERT_TIMEZONE('UTC','Asia/Kolkata',acq_ts_utc)) AS acq_date_ist,
   frp,
   bright_ti4,
   bright_ti5,
   scan,
   track,
   satellite_raw,
   satellite_std,
   instrument,
   confidence,
   version,
   processing_level,
   daynight,
   type AS fire_type,
   source,
   source_file,
   ingested_at,
   _loaded_at
   FROM deduped  WHERE rn =1