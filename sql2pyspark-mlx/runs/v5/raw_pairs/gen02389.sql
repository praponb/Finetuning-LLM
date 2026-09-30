SELECT
    s.id,
    s.sqft,
    s.city,
    t.last_sale_date
FROM stores s
LEFT JOIN (SELECT store_id, MAX(sale_date) last_sale_date
    FROM store_sales
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY city;
