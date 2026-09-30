SELECT id, unit_price, unit_price * quantity AS line_total
FROM order_items
WHERE quantity > 7 AND quantity >= 4;
