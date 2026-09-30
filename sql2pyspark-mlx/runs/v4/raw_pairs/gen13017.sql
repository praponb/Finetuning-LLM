SELECT MIN(unit_price) AS min_unit_price, ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM order_items
WHERE unit_price < 210
ORDER BY avg_unit_price DESC, 1
