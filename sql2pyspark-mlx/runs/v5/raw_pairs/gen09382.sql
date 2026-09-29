WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-05-18 19:00:00'
      AND method IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at, id) AS row_num
        FROM cleaned
    ) t
    WHERE row_num = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        paid_at_date,
        row_num,
        COUNT(amount) OVER (PARTITION BY method) AS method_count_amount,
        AVG(amount) OVER (ORDER BY row_num, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5
    FROM ranked
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        paid_at,
        row_num,
        method_count_amount,
        amount_avg_5,
        CAST(amount AS INT) AS amount_int,
        LEAST(amount, amount_avg_5) AS least_amount_amount_avg_5
    FROM with_windows
    WHERE order_id IS NOT NULL
      AND paid_at > DATE '2023-10-02'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        paid_at,
        method_count_amount,
        AVG(amount_avg_5) OVER (ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_5_avg_6_prior,
        AVG(least_amount_amount_avg_5) OVER (ORDER BY method_count_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS least_amount_amount_avg_5_avg_6,
        LEAD(amount, 1) OVER (ORDER BY method_count_amount, id) AS next_amount
    FROM base
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_amount >= 1740 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_amount_tier,
    APPROX_PERCENTILE(next_amount, 0.95) AS next_amount_p95,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes
FROM metrics
GROUP BY 1
HAVING (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 > 0;
