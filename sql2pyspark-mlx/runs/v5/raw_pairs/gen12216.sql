SELECT id, unit_price, quantity, SUM(quantity) OVER (ORDER BY unit_price, id) AS running_quantity FROM order_items
