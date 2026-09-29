SELECT
    CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    AVG(quantity) AS avg_quantity,
    MAX(quantity) AS max_quantity,
    MIN(unit_price) AS min_unit_price
FROM order_items
GROUP BY CASE WHEN quantity >= 8 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
HAVING avg_quantity < 3.0
ORDER BY min_unit_price DESC;
