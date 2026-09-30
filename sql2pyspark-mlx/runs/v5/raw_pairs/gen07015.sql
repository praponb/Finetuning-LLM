SELECT
    ss.id,
    ss.sale_date,
    ss.revenue,
    p.supplier_id,
    CONCAT(p.product_name, ' - ', p.category) AS product_name_category
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
