SELECT *
FROM (
    SELECT CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, order_id, AVG(unit_price) AS avg_unit_price
    FROM order_items
    GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END, order_id
) AS x
WHERE avg_unit_price < 340;
