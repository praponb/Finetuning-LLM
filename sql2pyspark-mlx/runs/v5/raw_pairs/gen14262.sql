SELECT
    ss.sale_date,
    p.product_name,
    p.category,
    s.country
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY s.country DESC;
