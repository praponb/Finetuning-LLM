SELECT product_id, ROUND(quantity * 0.9, 2) AS adjusted_quantity FROM order_items ORDER BY adjusted_quantity;
