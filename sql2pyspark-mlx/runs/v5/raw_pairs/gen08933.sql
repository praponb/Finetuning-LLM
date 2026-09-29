WITH cleaned AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        FLOOR(amount) AS amount_whole
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-07-24 04:00:00'
),
latest AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST, id) = 1
),
joined AS (
    SELECT
        e.id,
        e.order_id,
        e.method,
        d.order_date,
        d.shipping_fee,
        d.discount
    FROM latest e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
latest_payments AS (
    SELECT
        id,
        order_id,
        method,
        shipping_fee
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY discount, id) = 1
)
SELECT
    s.id,
    s.order_id,
    s.method,
    (SELECT MAX(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS max_payment_amount
FROM latest_payments s
ORDER BY order_id NULLS LAST
