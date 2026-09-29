SELECT id, brand FROM products WHERE category NOT IN ('Fashion', 'Electronics', 'Sports') ORDER BY brand, id DESC;
