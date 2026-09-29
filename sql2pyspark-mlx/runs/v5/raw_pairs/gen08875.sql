WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC) AS rank_in_group
    FROM order_items
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.rank_in_group,
    (SELECT AVG(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS avg_payment_amount
FROM ranked s;
