WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, value
),
parsed AS (
    -- Higher-order array functions
    SELECT
        service,
        host,
        attr_key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS has_fault
    FROM flattened
    WHERE attr_key IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        service,
        attr_key,
        value,
        status_code_high,
        has_fault,
        CAST(status_code_high AS DOUBLE) status_code_high_num,
        CASE WHEN service = 'auth' THEN 1 ELSE 0 END is_auth
    FROM parsed
    WHERE attr_key IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY is_auth DESC NULLS LAST) is_auth_rank,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high_num NULLS LAST) status_code_high_num_percent_rank,
        DENSE_RANK() OVER w_hist has_fault_dense_rank,
        CUME_DIST() OVER (PARTITION BY attr_key, service ORDER BY status_code_high NULLS FIRST) status_code_high_cume_dist,
        RANK() OVER w_hist has_fault_rank
    FROM filtered
    WINDOW w_hist AS (ORDER BY has_fault DESC NULLS LAST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY has_fault_rank NULLS LAST) has_fault_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY status_code_high_num_percent_rank NULLS LAST) status_code_high_num_percent_rank_percent_rank
    FROM metrics
),
metrics_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(is_auth_rank / SUM(is_auth_rank) OVER (PARTITION BY attr_key), 4) is_auth_rank_share,
        COUNT(is_auth_rank) OVER (PARTITION BY attr_key) attr_key_count_is_auth_rank,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY has_fault_rank DESC NULLS LAST) has_fault_rank_percent_rank_2,
        SUM(has_fault_rank_percent_rank) OVER (PARTITION BY attr_key, service) attr_key_sum_has_fault_rank_percent_rank,
        COUNT(status_code_high_num_percent_rank_percent_rank) OVER (PARTITION BY attr_key) attr_key_count_status_code_high_num_percent_rank_percent_rank
    FROM scored
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY status_code_high_num_percent_rank_percent_rank DESC NULLS LAST) status_code_high_num_percent_rank_percent_rank_percent_rank,
        DENSE_RANK() OVER w_ordered status_code_high_cume_dist_dense_rank,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY attr_key_count_status_code_high_num_percent_rank_percent_rank) attr_key_count_status_code_high_num_percent_rank_percent_rank_dense_rank,
        MIN(status_code_high_num) OVER (PARTITION BY attr_key) attr_key_min_status_code_high_num
    FROM metrics_app_logs
    WINDOW w_ordered AS (PARTITION BY attr_key ORDER BY status_code_high_cume_dist DESC NULLS FIRST)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        attr_key,
        service,
        is_auth_rank
    FROM enriched
) src
PIVOT (
    MAX(is_auth_rank) AS value_sum,
    COUNT(1) AS item_count
    FOR service IN ('billing' AS billing, 'search' AS search)
);
