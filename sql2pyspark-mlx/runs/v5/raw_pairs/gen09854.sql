SELECT a.id, a.store_id, a.product_id, a.sale_date, a.units, d.store_name FROM store_sales a INNER JOIN stores d ON a.store_id = d.id
