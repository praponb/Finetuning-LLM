SELECT id, quantity, ROW_NUMBER() OVER (ORDER BY quantity, id) AS rn
FROM order_items
ORDER BY quantity DESC;
