WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        LEAD(amount) OVER (PARTITION BY order_id ORDER BY amount, id) AS next_amount,
        LAG(amount) OVER (PARTITION BY order_id ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST) AS amount_cume_dist
    FROM payments
)
SELECT
    id,
    order_id,
    paid_at,
    status,
    prev_amount,
    row_seq
FROM metrics
