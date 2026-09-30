WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN key = 'version' THEN 1 ELSE 0 END AS is_version
    FROM flattened
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'version'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        status_code,
        key,
        attr_value,
        is_version,
        SUM(status_code) OVER w_recent AS status_code_sum_2h,
        AVG(status_code) OVER (PARTITION BY key, host) AS key_avg_status_code,
        MIN(status_code) OVER (PARTITION BY key) AS key_min_status_code,
        PERCENT_RANK() OVER w_ordered AS is_version_percent_rank
    FROM base
    WHERE HOUR(log_ts) BETWEEN 9 AND 12
    WINDOW w_recent AS (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY is_version)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(key_min_status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS key_min_status_code_count_2h,
        RANK() OVER (ORDER BY key_min_status_code) AS key_min_status_code_rank
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN is_version_percent_rank >= 0.72 THEN 'LARGE' ELSE 'SMALL' END AS is_version_percent_rank_tier,
    ROUND(SUM(key_min_status_code_count_2h) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS key_min_status_code_count_2h_per_trace,
    BIT_AND(status_code) AS status_code_bit_and,
    BIT_OR(status_code) AS status_code_bit_or,
    ROUND(SUM(key_min_status_code_rank) / NULLIF(COUNT(CASE WHEN key = 'region' THEN 1 END), 0), 2) AS key_min_status_code_rank_per_region,
    BIT_OR(status_code) AS status_code_bit_or_2
FROM enriched
GROUP BY CASE WHEN is_version_percent_rank >= 0.72 THEN 'LARGE' ELSE 'SMALL' END
