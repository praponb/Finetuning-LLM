SELECT price AS value FROM products UNION SELECT unit_price AS value FROM order_items WHERE product_id IS NULL ORDER BY value;
