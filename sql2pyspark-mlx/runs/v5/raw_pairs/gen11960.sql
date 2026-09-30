SELECT unit_price AS value FROM order_items WHERE product_id IN (8, 11, 31, 37) UNION ALL SELECT amount AS value FROM orders ORDER BY value
