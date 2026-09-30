SELECT id, product_id, order_id, quantity, PERCENT_RANK() OVER (PARTITION BY product_id, order_id ORDER BY quantity) AS quantity_pct_rank FROM order_items
