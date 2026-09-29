SELECT id, stock_qty, tags, SORT_ARRAY(tags) AS sorted_tags FROM products WHERE product_name LIKE 'O%' ORDER BY stock_qty
