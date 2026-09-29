SELECT category FROM products WHERE ARRAY_CONTAINS(tags, 'eco') UNION ALL SELECT category FROM products WHERE supplier_id = 10 ORDER BY category;
