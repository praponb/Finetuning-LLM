SELECT ss.revenue, p.product_name, p.supplier_id
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
