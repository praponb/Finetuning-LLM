SELECT id, product_id, sale_date FROM (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY sale_date DESC) AS pos FROM store_sales) AS r WHERE pos <= 1 ORDER BY product_id
