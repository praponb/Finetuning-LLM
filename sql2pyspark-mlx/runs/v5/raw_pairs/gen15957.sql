WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        DATE_TRUNC('MONTH', paid_at) AS paid_at_period,
        DATE_TRUNC('month', paid_at) AS paid_at_period_2
    FROM payments
    WHERE order_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        paid_at,
        paid_at_period_2,
        FIRST_VALUE(method) IGNORE NULLS OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY paid_at DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY paid_at_period_2 DESC, id) AS row_seq_2
    FROM valid
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT id, row_seq_2
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_seq_2 DESC, id) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(row_seq_2, 1) AS row_seq_2_rounded
    FROM latest
    WHERE row_seq_2 > 5
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_2_tier,
    CASE WHEN row_seq_2_rounded >= 67 THEN 'HIGH' ELSE 'LOW' END AS row_seq_2_rounded_tier,
    ROUND(AVG(row_seq_2), 2) AS avg_row_seq_2,
    MIN(row_seq_2) AS min_row_seq_2,
    APPROX_PERCENTILE(row_seq_2_rounded, 0.95) AS row_seq_2_rounded_p95,
    GROUPING(CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM base
GROUP BY GROUPING SETS ((CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN row_seq_2_rounded >= 67 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END));
