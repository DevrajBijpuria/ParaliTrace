WITH delhi AS (
    SELECT lat AS delhi_lat, lon AS delhi_lon
    FROM {{ ref('route_points') }}
    WHERE point_key = 'delhi'
),

base AS (
    SELECT
        s.station_key,
        s.name AS station_name,
        s.location_id,
        s.pm25_sensor_id,
        s.lat,
        s.lon,
        s.zone,
        s.available_from,
        s.backfill_role,
        (s.backfill_role = 'full') AS is_forecast_target,
        ROUND(haversine(s.lat, s.lon, d.delhi_lat, d.delhi_lon), 2) AS distance_to_delhi_center_km
    FROM {{ ref('stations') }} s
    CROSS JOIN delhi d
),

with_region AS (
    SELECT
        b.*,
        COALESCE(r.region_key, 'other') AS region_key
    FROM base b
    LEFT JOIN {{ ref('regions') }} r
        ON  b.lat BETWEEN r.min_lat AND r.max_lat
        AND b.lon BETWEEN r.min_lon AND r.max_lon
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.station_key ORDER BY r.priority) = 1
)

SELECT * FROM with_region