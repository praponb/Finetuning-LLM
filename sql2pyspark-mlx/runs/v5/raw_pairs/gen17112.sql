WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS row_num FROM stores WHERE LENGTH(store_name) < 8) SELECT id, region, sqft FROM numbered WHERE row_num = 1;
