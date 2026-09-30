SELECT
    CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END quantity_bucket,
    SUM(unit_price * quantity) revenue,
    ROUND(STDDEV(quantity), 2) stddev_quantity,
    ROUND(STDDEV(unit_price), 2) stddev_unit_price
FROM order_items
GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END;
