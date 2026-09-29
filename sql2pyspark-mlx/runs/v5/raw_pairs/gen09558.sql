SELECT cost, product_name, stock_qty, INITCAP(TRIM(category)) AS category_title, ARRAY_JOIN(tags, ', ') AS tags_text FROM products
