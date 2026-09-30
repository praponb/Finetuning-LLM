SELECT cost FROM products INTERSECT SELECT unit_price FROM order_items WHERE product_id IS NULL ORDER BY cost;
