SELECT
    p.cost,
    ss.sale_date,
    ss.store_id,
    ss.product_id
FROM products p
LEFT OUTER JOIN store_sales ss ON ss.product_id = p.id
