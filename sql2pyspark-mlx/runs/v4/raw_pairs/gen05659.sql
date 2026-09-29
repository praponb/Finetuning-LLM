SELECT
    ss.revenue,
    ss.product_id,
    s.store_name,
    p.cost
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
