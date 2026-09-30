SELECT *
FROM (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    WHERE product_id IS NULL
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END, order_id
) AS x
WHERE stddev_unit_price < 78
