SELECT brand, ROUND(AVG(price), 2) avg_price, SORT_ARRAY(COLLECT_SET(category)) category_list
FROM products
GROUP BY brand;
