SELECT
    id,
    unit_price,
    order_id,
    ROUND(unit_price, 2) AS unit_price_rounded
FROM order_items
WHERE NOT (order_id IN (2, 20, 34) OR product_id IN (21, 25, 29));
