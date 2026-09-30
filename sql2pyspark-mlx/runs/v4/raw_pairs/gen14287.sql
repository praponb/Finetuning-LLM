SELECT cost FROM products INTERSECT SELECT shipping_fee AS cost FROM orders ORDER BY cost;
