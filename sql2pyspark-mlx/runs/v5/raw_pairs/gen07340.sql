SELECT supplier_id FROM products EXCEPT DISTINCT SELECT supplier_id FROM products WHERE ARRAY_CONTAINS(tags, 'new')
