SELECT order_id, MAX(quantity) AS max_quantity, COUNT(*) AS num_order_items FROM order_items GROUP BY 1;
