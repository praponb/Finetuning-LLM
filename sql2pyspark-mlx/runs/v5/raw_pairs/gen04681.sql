SELECT order_id, product_id, FLOOR(quantity) AS quantity_whole FROM order_items WHERE unit_price >= 390;
