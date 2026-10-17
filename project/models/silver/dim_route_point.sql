WITH delhi AS (
    SELECT lat AS delhi_lat, lon AS delhi_lon
    from {{ ref('route_points') }}
    where point_key = 'delhi'
),

calc AS (
    SELECT
        r.point_key,
        r.name,
        r.lat,
        r.lon,
        r.segment,
        r.route_order,
        haversine(r.lat, r.lon, d.delhi_lat, d.delhi_lon) AS distance_to_delhi_km,
        mod(degrees(atan2(
                sin(radians(d.delhi_lon - r.lon)) * cos(radians(d.delhi_lat)),
                cos(radians(r.lat)) * sin(radians(d.delhi_lat))
                  - sin(radians(r.lat)) * cos(radians(d.delhi_lat)) * cos(radians(d.delhi_lon - r.lon)))) + 360,
            360  )  as bearing_to_delhi_deg
    FROM {{ ref('route_points') }} r
    CROSS JOIN  delhi d ),

base AS (
    SELECT 
        point_key,
        name as point_name,
        lat,
        lon,
        segment,
        route_order,
        (segment = 'receptor') as is_receptor,
        round(distance_to_delhi_km, 2) as distance_to_delhi_km,
        case when segment = 'receptor' then null
             else round(bearing_to_delhi_deg, 1)
        end    as bearing_to_delhi_deg,
        case when segment = 'receptor' then null
             else round(mod(bearing_to_delhi_deg + 180, 360), 1)
        end    as transport_wind_from_deg
    FROM calc
),

with_region AS (
    SELECT
        b.*,
        COALESCE(r.region_key, 'other') AS region_key
    FROM base b
    LEFT JOIN {{ ref('regions') }} r
        ON  b.lat BETWEEN r.min_lat AND r.max_lat
        AND b.lon BETWEEN r.min_lon AND r.max_lon
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.point_key ORDER BY r.priority) = 1
)

SELECT * FROM with_region