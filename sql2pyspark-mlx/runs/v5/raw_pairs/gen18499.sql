SELECT *, unit_price + quantity AS line_total
FROM order_items
ORDER BY line_total DESC, id ASC
LIMIT 50;
