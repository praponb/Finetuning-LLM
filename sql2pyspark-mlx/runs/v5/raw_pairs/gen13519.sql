SELECT id, cost, supplier_id FROM products WHERE cost >= (SELECT MAX(cost) FROM products WHERE category IN ('Home', 'Books'));
