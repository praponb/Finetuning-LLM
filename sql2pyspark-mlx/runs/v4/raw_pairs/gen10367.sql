SELECT
    CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END AS quantity_bucket,
    CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
    AVG(quantity) AS avg_quantity,
    MAX(quantity) AS max_quantity,
    MAX(unit_price) AS max_unit_price
FROM order_items
GROUP BY CASE WHEN quantity >= 9 THEN 'high' WHEN quantity >= 4 THEN 'mid' ELSE 'low' END, CASE WHEN unit_price >= 340 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END;
