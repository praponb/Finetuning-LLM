SELECT
    ss.store_id,
    ss.sale_date,
    ss.revenue,
    p.cost
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
