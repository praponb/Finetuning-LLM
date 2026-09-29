SELECT brand, category, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values FROM products GROUP BY brand, category
