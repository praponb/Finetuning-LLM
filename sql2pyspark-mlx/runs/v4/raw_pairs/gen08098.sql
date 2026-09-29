SELECT e.*, b.product_name, b.category FROM order_items e INNER JOIN products b ON e.product_id = b.id;
