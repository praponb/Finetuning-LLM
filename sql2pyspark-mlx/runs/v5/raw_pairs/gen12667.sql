SELECT ROUND(AVG(stock_qty), 2) AS avg_stock_qty, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
FROM products
WHERE category NOT IN ('Home', 'Grocery', 'Electronics')
ORDER BY brand_values;
