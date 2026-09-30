SELECT SORT_ARRAY(COLLECT_SET(brand)) AS brand_list, MIN(price) AS min_price, AVG(price) AS avg_price
FROM products
ORDER BY avg_price DESC;
