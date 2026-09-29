SELECT product_id, unit_price_bucket, total_unit_price
FROM (
    SELECT product_id, CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 50 THEN 'mid' ELSE 'low' END AS unit_price_bucket, SUM(unit_price) AS total_unit_price
    FROM order_items
    GROUP BY product_id, CASE WHEN unit_price >= 380 THEN 'high' WHEN unit_price >= 50 THEN 'mid' ELSE 'low' END
) AS x
WHERE total_unit_price > 1100;
