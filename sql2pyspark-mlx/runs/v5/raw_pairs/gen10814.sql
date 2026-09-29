WITH top_products AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS row_num FROM products) SELECT * FROM top_products WHERE row_num = 1;
