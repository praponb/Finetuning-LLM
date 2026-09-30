SELECT cost, supplier_id, ARRAY_JOIN(tags, ', ') AS tags_text FROM products ORDER BY tags_text;
