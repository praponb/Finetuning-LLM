SELECT
    brand,
    MONTH(launch_date) AS launch_month,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SUM(CASE WHEN brand = 'Globex' THEN price ELSE 0 END) AS globex_price,
    SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END) AS acme_count
FROM products
WHERE product_name LIKE 'Bac%'
GROUP BY brand, MONTH(launch_date);
