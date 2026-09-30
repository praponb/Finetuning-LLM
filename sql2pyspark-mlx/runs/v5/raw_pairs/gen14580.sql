SELECT ss.id, ss.sale_date FROM store_sales ss WHERE ss.product_id NOT IN (SELECT id FROM products WHERE category IN ('Electronics', 'Grocery', 'Books'));
