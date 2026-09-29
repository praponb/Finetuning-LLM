SELECT ss.store_id, p.launch_date, s.country
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE UPPER(TRIM(p.product_name)) = 'RUNNING SHOES';
