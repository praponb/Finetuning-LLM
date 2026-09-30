SELECT id, supplier_id, category, price, FLOOR(cost) AS cost_whole FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller');
