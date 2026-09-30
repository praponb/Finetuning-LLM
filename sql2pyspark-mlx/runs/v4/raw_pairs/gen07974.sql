SELECT id, order_id, quantity FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS rnk FROM order_items WHERE quantity >= 7) AS r WHERE rnk = 1 ORDER BY quantity
