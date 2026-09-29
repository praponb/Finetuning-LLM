SELECT id, supplier_id, stock_qty, category, FIRST_VALUE(category) OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC, id) AS first_category FROM products ORDER BY supplier_id DESC, id ASC;
