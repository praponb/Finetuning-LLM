SELECT quantity_bucket, unique_products
FROM (
    SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(DISTINCT unit_price) AS unique_products
    FROM order_items
    GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
) AS x
WHERE unique_products >= 3
