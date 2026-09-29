WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) AS latest_rank
    FROM orders
    QUALIFY latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.amount,
    s.shipping_fee,
    (SELECT SUM(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_discount
FROM ranked s
ORDER BY customer_id DESC NULLS FIRST
