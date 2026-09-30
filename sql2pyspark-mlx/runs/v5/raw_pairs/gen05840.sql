SELECT quantity, product_id, order_id, ROUND(quantity / unit_price, 2) AS line_total FROM order_items;
