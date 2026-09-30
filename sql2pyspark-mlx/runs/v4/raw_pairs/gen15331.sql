SELECT quantity_bucket, quantity_range
FROM (
    SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM order_items
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
) AS agg
WHERE quantity_range < 74
ORDER BY 2, quantity_bucket;
