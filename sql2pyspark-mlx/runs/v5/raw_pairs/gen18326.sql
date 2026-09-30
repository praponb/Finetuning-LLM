SELECT id, quantity, CEIL(unit_price) AS unit_price_whole
FROM order_items
WHERE unit_price >= 220 AND quantity >= 5 AND order_id IN (26, 34, 36)
