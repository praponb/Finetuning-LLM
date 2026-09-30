SELECT id, unit_price, quantity
FROM order_items
WHERE unit_price >= (SELECT MAX(unit_price) FROM order_items)
ORDER BY unit_price
