WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        host,
        status_code,
        CAST((status_code & 4) != 0 AS INT) AS flag_bit4,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM exploded
    WHERE host IN ('h1', 'h4')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        status_code_high,
        CUME_DIST() OVER (ORDER BY flag_bit4 ASC NULLS LAST) AS flag_bit4_cume_dist,
        RANK() OVER (PARTITION BY host ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
        STDDEV_SAMP(status_code_high) OVER (PARTITION BY host) AS host_stddev_samp_status_code_high,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY flag_bit4 DESC NULLS FIRST) AS flag_bit4_dense_rank
    FROM unpacked
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        status_code_high,
        flag_bit4_cume_dist,
        status_code_high_rank,
        flag_bit4_dense_rank,
        status_code_high_rank / NULLIF(status_code_high, 0) AS status_code_high_rank_per_status_code_high,
        ROUND(host_stddev_samp_status_code_high / flag_bit4_dense_rank, 2) AS host_stddev_samp_status_code_high_per_flag_bit4_dense_rank
    FROM scored
    WHERE host_stddev_samp_status_code_high < 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_high_rank_per_status_code_high >= 73 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_rank_per_status_code_high_tier,
    MIN(status_code_high_rank) AS min_status_code_high_rank,
    PERCENTILE_APPROX(flag_bit4_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS flag_bit4_dense_rank_quartiles
FROM valid
GROUP BY CASE WHEN status_code_high_rank_per_status_code_high >= 73 THEN 'LARGE' ELSE 'SMALL' END
HAVING MIN(status_code_high_rank) >= 0;
