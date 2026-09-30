WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM payments
    WHERE paid_at > DATE '2023-11-30'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        amount_int,
        SUBSTRING(method, 1, 2) AS method_prefix,
        COALESCE(method, 'N/A') AS method_clean
    FROM cleaned
    WHERE paid_at >= DATE '2024-01-26'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount_int) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_int_sum_30d,
        SUM(amount_int) OVER (PARTITION BY order_id, status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_int,
        AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        FIRST_VALUE(method) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        NTILE(5) OVER (PARTITION BY order_id, method ORDER BY amount, id) AS amount_bucket
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Window metrics over several different partitions
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    amount_int,
    method_prefix,
    method_clean,
    amount_int_sum_30d,
    running_amount_int,
    amount_avg_3,
    first_known_method,
    amount_bucket,
    new_group_flag,
    burst_id,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount_bucket, id) AS seq_no,
    MAX(amount_bucket) OVER w_running AS amount_bucket_max_90d
FROM sessionized
WINDOW w_running AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW);
