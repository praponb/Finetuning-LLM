SELECT
    s.id,
    s.region,
    s.opened_date,
    s.city
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.sale_date < '2024-12-01');
