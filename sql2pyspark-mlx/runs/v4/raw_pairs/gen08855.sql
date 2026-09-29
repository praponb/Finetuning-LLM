SELECT id, product_id, order_id, CEIL(unit_price) AS unit_price_whole FROM order_items WHERE product_id IN (4, 11, 15, 34) AND order_id IN (5, 6, 31, 38) AND product_id = 15;
