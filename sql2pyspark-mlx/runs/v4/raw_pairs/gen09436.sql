SELECT ROUND(AVG(unit_price), 2) AS avg_unit_price, MAX(unit_price) - MIN(unit_price) AS unit_price_range, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
WHERE order_id IN (10, 18, 22, 31)
ORDER BY unit_price_range DESC;
