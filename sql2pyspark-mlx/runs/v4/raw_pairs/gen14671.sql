WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY amount DESC, id) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount NULLS LAST) AS amount_dense_rank,
        AVG(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_90d,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM payments
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.amount AS order_amount,
        p.discount
    FROM scored s
    JOIN orders p
        ON s.order_id = p.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    row_seq,
    amount_dense_rank,
    amount_avg_90d,
    last_known_amount
FROM islands
ORDER BY last_known_amount DESC NULLS LAST
