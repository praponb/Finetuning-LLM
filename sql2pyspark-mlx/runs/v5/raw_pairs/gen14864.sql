SELECT unit_price FROM order_items WHERE product_id = 4 UNION ALL SELECT amount AS unit_price FROM transactions;
