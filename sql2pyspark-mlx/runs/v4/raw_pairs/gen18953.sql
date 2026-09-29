WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS pos FROM store_sales) SELECT id, product_id, sale_date FROM numbered WHERE pos <= 1;
