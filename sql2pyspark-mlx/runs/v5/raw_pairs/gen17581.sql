WITH lines AS (
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, value
),
metrics AS (
    SELECT
        id,
        status_code,
        key,
        SUM(latency_ms) OVER (PARTITION BY key, service) AS key_sum_latency_ms,
        RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank
    FROM lines
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(key)) AS key_title,
        COALESCE(key, 'UNKNOWN') AS key_filled
    FROM metrics
    WHERE key IS NOT DISTINCT FROM 'version'
      AND latency_ms_rank > 6
),
cleaned_app_logs AS (
    SELECT
        id,
        status_code,
        key,
        key_sum_latency_ms,
        latency_ms_rank,
        key_title,
        key_filled,
        FLOOR(status_code) AS status_code_whole,
        COALESCE(key, 'UNKNOWN') AS key_clean
    FROM cleaned
    WHERE key IS NOT NULL
),
filtered AS (
    SELECT
        *,
        ROUND(key_sum_latency_ms * 1.15, 2) AS key_sum_latency_ms_scaled,
        ROUND(latency_ms_rank * 0.85, 2) AS latency_ms_rank_adjusted
    FROM cleaned_app_logs
    WHERE key IS NOT NULL
)
SELECT
    key,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list_2
FROM filtered
GROUP BY key
ORDER BY key DESC
