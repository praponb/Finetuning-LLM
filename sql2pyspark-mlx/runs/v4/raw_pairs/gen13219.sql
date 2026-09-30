SELECT i.id, i.store_id, i.product_id, i.last_restock FROM inventory i WHERE i.product_id IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'bestseller'));
