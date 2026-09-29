WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN amount >= 1540 THEN 'High' WHEN amount >= 510 THEN 'Medium' ELSE 'Low' END AS amount_level,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE paid_at > TO_DATE('2023-06-18')
      AND method IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY method ORDER BY amount) AS amount_rank,
        NTILE(3) OVER (PARTITION BY method ORDER BY amount DESC, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY status, method) AS status_sum_amount,
        FIRST_VALUE(amount) IGNORE NULLS OVER w_ordered AS first_known_amount,
        NTILE(3) OVER (PARTITION BY order_id, status ORDER BY amount, id) AS amount_bucket_2
    FROM base
    WINDOW w_ordered AS (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    amount_level,
    method_filled,
    amount_rank,
    amount_bucket,
    status_sum_amount,
    first_known_amount,
    amount_bucket_2,
    is_new_burst,
    session_id,
    FIRST_VALUE(amount_rank, TRUE) OVER (PARTITION BY status, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_rank,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_rank, id) AS row_seq
FROM islands;
