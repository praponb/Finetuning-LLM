SELECT id, product_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS quantity_row FROM order_items WHERE unit_price >= 240;
