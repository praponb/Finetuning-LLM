SELECT id, quantity, ROUND(quantity * 0.9, 2) AS quantity_adjusted FROM order_items ORDER BY quantity_adjusted DESC, id DESC LIMIT 5;
