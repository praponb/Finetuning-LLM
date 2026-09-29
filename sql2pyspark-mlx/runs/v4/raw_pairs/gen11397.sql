WITH metrics AS (
    SELECT
        *,
        NTILE(10) OVER w_running AS amount_bucket,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method
    FROM payments
    WINDOW w_running AS (PARTITION BY method ORDER BY amount DESC, id)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at NULLS FIRST, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM deduped
),
flagged AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT *
FROM (
    SELECT
        method,
        order_id,
        status,
        amount_bucket
    FROM sessionized
) src
PIVOT (
    SUM(amount_bucket) AS amount
    FOR status IN ('SUCCESS' AS success, 'FAILED' AS failed)
)
