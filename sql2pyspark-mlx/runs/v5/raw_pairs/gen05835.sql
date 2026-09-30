SELECT
    s.id,
    s.store_name,
    COUNT(ss.id) AS store_sale_count,
    MAX(ss.sale_date) AS last_sale_date
FROM stores s
LEFT JOIN store_sales ss ON ss.store_id = s.id
GROUP BY s.id, s.store_name
ORDER BY last_sale_date;
