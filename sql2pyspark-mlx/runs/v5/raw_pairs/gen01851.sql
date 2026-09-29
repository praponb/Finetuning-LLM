SELECT id, unit_price
FROM order_items
WHERE unit_price < (SELECT MIN(unit_price) FROM order_items)
ORDER BY unit_price DESC, id
LIMIT 5;
