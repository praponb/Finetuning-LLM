SELECT
    id,
    product_id,
    order_id,
    CASE WHEN unit_price >= 350 THEN 'High' WHEN unit_price >= 120 THEN 'Medium' ELSE 'Low' END AS unit_price_tier,
    FLOOR(quantity) AS quantity_whole
FROM order_items
WHERE (unit_price BETWEEN 240 AND 270 AND unit_price BETWEEN 130 AND 400) OR order_id IN (36, 38, 39);
