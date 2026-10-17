{{ config(
    materialized = 'incremental',
    unique_key = 'fire_id',
    incremental_strategy = 'merge'
) }}

WITH base AS (
    SELECT
        *,
        CONVERT_TIMEZONE('Asia/Kolkata', 'UTC', DATE_TRUNC('hour', acq_ts_ist))::TIMESTAMP_NTZ AS hour_start_utc,
        ROUND(FLOOR(latitude  / 0.004) * 0.004 + 0.002, 4)                                   AS cell_lat,
        ROUND(FLOOR(longitude / 0.004) * 0.004 + 0.002, 4)                                   AS cell_lon
    FROM {{ ref('stg_fires') }}
    WHERE latitude  BETWEEN 27.6 AND 32.6
      AND longitude BETWEEN 73.8 AND 77.6
      AND (fire_type IS NULL OR fire_type = 0)
),

filtered AS (
    SELECT * FROM base
    {% if is_incremental() %}
    WHERE hour_start_utc >= (SELECT DATEADD(day, -2, MAX(hour_start_utc)) FROM {{ this }})
    {% endif %}
),

per_satellite AS (
    SELECT
        cell_lat,
        cell_lon,
        hour_start_utc,
        satellite_std,
        SUM(frp)                                                        AS frp_sum,
        COUNT(*)                                                        AS n_pixels,
        MIN(acq_ts_utc)                                                 AS first_ts,
        AVG(latitude)                                                   AS lat,
        AVG(longitude)                                                  AS lon,
        MAX(CASE confidence WHEN 'h' THEN 3 WHEN 'n' THEN 2 ELSE 1 END) AS conf_rank,
        MAX(IFF(processing_level = 'NRT', 1, 0))                        AS has_nrt
    FROM filtered
    GROUP BY cell_lat, cell_lon, hour_start_utc, satellite_std
),

events AS (
    SELECT
        MD5(CONCAT_WS('|', cell_lat, cell_lon, hour_start_utc))          AS fire_id,
        cell_lat,
        cell_lon,
        hour_start_utc,
        MIN(first_ts)                                                    AS first_acq_ts_utc,
        AVG(lat)                                                         AS latitude,
        AVG(lon)                                                         AS longitude,
        MAX(frp_sum)                                                     AS frp_mw,
        SUM(n_pixels)                                                    AS n_detections,
        COUNT(*)                                                         AS n_satellites,
        LISTAGG(satellite_std, ',') WITHIN GROUP (ORDER BY satellite_std) AS satellites,
        MAX(conf_rank)                                                   AS conf_rank,
        MAX(has_nrt)                                                     AS has_nrt
    FROM per_satellite
    GROUP BY cell_lat, cell_lon, hour_start_utc
),

with_region AS (
    SELECT
        e.*,
        r.region_key
    FROM events e
    LEFT JOIN {{ ref('regions') }} r
      ON e.latitude  BETWEEN r.min_lat AND r.max_lat
     AND e.longitude BETWEEN r.min_lon AND r.max_lon
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.fire_id ORDER BY r.priority NULLS LAST) = 1
)

SELECT
    fire_id,
    hour_start_utc,
    CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hour_start_utc)::TIMESTAMP_NTZ  AS hour_start_ist,
    TO_DATE(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', hour_start_utc))        AS date_ist,
    first_acq_ts_utc,
    latitude,
    longitude,
    cell_lat,
    cell_lon,
    COALESCE(region_key, 'other')                                           AS region_key,
    frp_mw,
    n_detections,
    n_satellites,
    satellites,
    CASE conf_rank WHEN 3 THEN 'h' WHEN 2 THEN 'n' ELSE 'l' END             AS confidence_max,
    (conf_rank = 1)                                                         AS is_low_confidence,
    IFF(has_nrt = 1, 'NRT', 'SP')                                           AS processing_level
FROM with_region