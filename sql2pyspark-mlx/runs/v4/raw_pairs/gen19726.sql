SELECT category, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, COUNT(stock_qty) AS stock_qty_count FROM products WHERE brand = 'Umbrella' GROUP BY category
