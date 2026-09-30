WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(key)) AS key_title
    FROM lines
    WHERE key IS NOT NULL
      AND log_ts >= '2025-05-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        key,
        value,
        LOWER(key) AS key_lower,
        LENGTH(level) AS level_length
    FROM filtered
    WHERE log_ts >= TO_DATE('2025-06-01')
      AND key IS NOT NULL
),
filtered_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        service,
        key,
        value,
        level_length,
        CASE WHEN service = 'search' THEN 1 ELSE 0 END AS is_search
    FROM base
    WHERE key IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    value,
    level_length,
    is_search,
    PERCENT_RANK() OVER (ORDER BY is_search NULLS LAST) AS is_search_percent_rank,
    CUME_DIST() OVER (ORDER BY level_length DESC) AS level_length_cume_dist,
    RANK() OVER (PARTITION BY service ORDER BY level_length DESC) AS level_length_rank,
    PERCENT_RANK() OVER (PARTITION BY service ORDER BY is_search DESC NULLS LAST) AS is_search_percent_rank_2
FROM filtered_app_logs;
