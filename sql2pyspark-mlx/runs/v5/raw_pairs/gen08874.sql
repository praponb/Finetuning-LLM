SELECT *, RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC NULLS FIRST) AS latest_rank FROM order_items QUALIFY latest_rank = 1
