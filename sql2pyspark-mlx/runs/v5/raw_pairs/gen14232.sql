WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS row_num FROM store_sales) SELECT * FROM ranked WHERE row_num <= 2
