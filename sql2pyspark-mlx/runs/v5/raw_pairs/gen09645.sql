WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        status,
        AVG(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        LEAD(amount, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY method, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        PERCENT_RANK() OVER (PARTITION BY order_id, status ORDER BY amount DESC) AS amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY amount DESC, id) AS event_seq
    FROM payments
)
SELECT
    s.id,
    s.order_id,
    s.status,
    s.amount_avg_2,
    s.next_amount,
    s.first_known_amount,
    s.amount_percent_rank,
    s.event_seq,
    (SELECT AVG(t2.quantity) FROM order_items t2 WHERE t2.order_id = s.order_id) AS avg_order_item_quantity
FROM scored s;
