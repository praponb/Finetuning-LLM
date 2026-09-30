SELECT product_id, unit_price_bucket, unique_products
FROM (
    SELECT product_id, CASE WHEN unit_price >= 310 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(DISTINCT quantity) AS unique_products
    FROM order_items
    WHERE product_id = 4
    GROUP BY product_id, CASE WHEN unit_price >= 310 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END
) AS t
WHERE unique_products >= 9;
