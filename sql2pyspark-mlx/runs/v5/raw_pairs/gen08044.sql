SELECT s.* FROM suppliers s WHERE EXISTS (SELECT 1 FROM products c WHERE c.supplier_id = s.id AND c.category IN ('Grocery', 'Electronics', 'Sports'));
