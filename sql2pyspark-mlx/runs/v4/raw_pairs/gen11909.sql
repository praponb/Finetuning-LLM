SELECT id, store_id, sale_date, product_id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS row_num FROM store_sales WHERE units > 20) AS t WHERE row_num <= 3
