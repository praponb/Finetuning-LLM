WITH with_latest AS (
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.amount,
        s.discount,
        latest.payment_paid_at
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.status,
    s.amount,
    s.discount,
    (SELECT SUM(s2.revenue) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS sum_store_sale_revenue
FROM with_latest s
ORDER BY store_id DESC NULLS FIRST, sum_store_sale_revenue;
