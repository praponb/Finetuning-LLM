WITH enriched AS (
    SELECT
        *,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 4) AS quantity_share_2
    FROM order_items
    WHERE unit_price BETWEEN 180 AND 220
)
SELECT
    s.id,
    s.order_id,
    s.unit_price,
    s.quantity_share,
    (SELECT MIN(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS min_payment_amount
FROM enriched s
ORDER BY unit_price ASC NULLS FIRST;
