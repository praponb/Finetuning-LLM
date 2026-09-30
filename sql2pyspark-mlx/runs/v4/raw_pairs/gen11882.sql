WITH top_stores AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS row_num FROM stores) SELECT * FROM top_stores WHERE row_num = 1 ORDER BY city, sqft DESC
