SELECT
    ss.sale_date,
    s.opened_date,
    p.category,
    p.brand
FROM store_sales ss
RIGHT OUTER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
