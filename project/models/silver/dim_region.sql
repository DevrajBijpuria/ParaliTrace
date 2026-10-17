SELECT region_key, region_name, priority, min_lat, max_lat, min_lon, max_lon
FROM {{ ref('regions') }}

UNION ALL

SELECT 'other', 'Outside study regions', 99, NULL, NULL, NULL, NULL