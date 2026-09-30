WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        latency_ms,
        status_code,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM exploded
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(flag_bit1 * 100, 2) flag_bit1_adjusted
    FROM flattened_attrs
    WHERE flag_bit1 BETWEEN 0 AND 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        latency_ms,
        status_code,
        attr_value,
        status_code_high,
        flag_bit1_adjusted,
        flag_bit1 + status_code_high flag_bit1_plus_status_code_high
    FROM base
    WHERE status_code_high < 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY flag_bit1_plus_status_code_high NULLS LAST) flag_bit1_plus_status_code_high_percent_rank,
        CUME_DIST() OVER (ORDER BY status_code DESC) status_code_cume_dist,
        DENSE_RANK() OVER (ORDER BY flag_bit1_plus_status_code_high NULLS LAST) flag_bit1_plus_status_code_high_dense_rank,
        RANK() OVER (ORDER BY status_code_high DESC NULLS FIRST) status_code_high_rank
    FROM cleaned
),
scored AS (
    -- Running and moving aggregates
    SELECT
        latency_ms,
        status_code,
        attr_value,
        status_code_high,
        flag_bit1_plus_status_code_high,
        flag_bit1_plus_status_code_high_dense_rank,
        status_code_high_rank,
        DENSE_RANK() OVER (ORDER BY flag_bit1_plus_status_code_high DESC NULLS LAST) flag_bit1_plus_status_code_high_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY status_code_high_rank DESC NULLS FIRST) status_code_high_rank_dense_rank
    FROM metrics
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(status_code_high_rank_dense_rank AS DOUBLE) status_code_high_rank_dense_rank_num
    FROM scored
    WHERE flag_bit1_plus_status_code_high < 58
)
-- Grouped metrics
SELECT
    CASE WHEN status_code_high_rank_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_rank_dense_rank_tier,
    CASE WHEN status_code_high_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_rank_tier,
    CASE WHEN flag_bit1_plus_status_code_high >= 79 THEN 'LARGE' ELSE 'SMALL' END AS flag_bit1_plus_status_code_high_tier,
    PERCENTILE_APPROX(flag_bit1_plus_status_code_high, ARRAY(0.25, 0.5, 0.75)) AS flag_bit1_plus_status_code_high_quartiles,
    COUNT(*) AS cnt,
    ROUND(AVG(status_code_high_rank_dense_rank_num), 2) AS avg_status_code_high_rank_dense_rank_num
FROM filtered
GROUP BY status_code_high_rank_dense_rank_tier, status_code_high_rank_tier, flag_bit1_plus_status_code_high_tier
HAVING ROUND(AVG(status_code_high_rank_dense_rank_num), 2) > 0;
