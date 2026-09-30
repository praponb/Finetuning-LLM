SELECT ss.id, ss.units, p.supplier_id
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id;
