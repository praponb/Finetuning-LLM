SELECT
    s.store_name,
    s.city,
    ss.revenue,
    ss.units,
    ss.store_id
FROM stores s
JOIN store_sales ss ON ss.store_id = s.id
WHERE s.opened_date > '2024-12-26' OR ss.sale_date < '2023-03-01'
ORDER BY ss.store_id DESC
