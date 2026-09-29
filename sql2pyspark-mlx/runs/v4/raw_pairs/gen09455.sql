SELECT
    ss.product_id,
    p.launch_date,
    s.supplier_name,
    s.country
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id;
