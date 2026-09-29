SELECT supplier_id, category FROM products EXCEPT DISTINCT SELECT supplier_id, category FROM products WHERE category IN ('Home', 'Books')
