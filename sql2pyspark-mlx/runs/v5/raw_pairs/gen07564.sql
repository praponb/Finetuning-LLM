SELECT p.price, i.product_id FROM products p LEFT JOIN inventory i ON i.product_id = p.id;
