WITH dlq AS (
    SELECT
        source,
        COALESCE(record:error_reason::STRING, 'unknown') AS error_reason,
        _loaded_at
    FROM {{ source('raw', 'DEAD_LETTER_BRONZE') }}
),

clean AS (
    SELECT 'firms' AS source, COUNT(*) AS clean_rows FROM {{ source('raw', 'FIRES_BRONZE') }}
    UNION ALL
    SELECT 'openaq' AS source, COUNT(*) AS clean_rows FROM {{ source('raw', 'AQ_BRONZE') }}
    UNION ALL
    SELECT 'openmeteo' AS source, COUNT(*) AS clean_rows FROM {{ source('raw', 'WEATHER_BRONZE') }}
),

by_reason AS (
    SELECT
        source,
        error_reason,
        COUNT(*) AS rejected_rows,
        MIN(_loaded_at) AS first_seen_at,
        MAX(_loaded_at) AS last_seen_at
    FROM dlq
    GROUP BY source, error_reason
)

SELECT
    c.source,
    COALESCE(r.error_reason, 'none') AS error_reason,
    COALESCE(r.rejected_rows, 0) AS rejected_rows,
    c.clean_rows,
    SUM(COALESCE(r.rejected_rows, 0)) OVER (PARTITION BY c.source) AS total_rejected_rows,
    ROUND(100 * COALESCE(r.rejected_rows, 0)
          / NULLIF(c.clean_rows + SUM(COALESCE(r.rejected_rows, 0)) OVER (PARTITION BY c.source), 0), 3) AS reject_pct,
    r.first_seen_at,
    r.last_seen_at
FROM clean c
LEFT JOIN by_reason r ON r.source = c.source