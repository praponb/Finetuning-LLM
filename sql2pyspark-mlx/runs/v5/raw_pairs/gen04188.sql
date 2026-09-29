SELECT id, product_id, quantity, PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY quantity) quantity_pct_rank FROM order_items WHERE unit_price >= 210
