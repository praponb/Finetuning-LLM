SELECT
    s.store_name,
    s.opened_date,
    ss.units,
    ss.store_id
FROM stores s
INNER JOIN store_sales ss ON ss.store_id = s.id
