WITH loads AS (
    SELECT 'firms' AS source, _file_name, _loaded_at
    FROM {{ source('raw', 'FIRES_BRONZE') }}

    UNION ALL

    SELECT 'openaq' AS source, _file_name, _loaded_at
    FROM {{ source('raw', 'AQ_BRONZE') }}

    UNION ALL

    SELECT 'openmeteo' AS source, _file_name, _loaded_at
    FROM {{ source('raw', 'WEATHER_BRONZE') }}
)

SELECT
    source,
    DATE(_loaded_at) AS load_date,
    COUNT(DISTINCT _file_name) AS files_loaded,
    COUNT(*) AS rows_loaded,
    MIN(_loaded_at) AS first_loaded_at,
    MAX(_loaded_at) AS last_loaded_at
FROM loads
GROUP BY source, DATE(_loaded_at)