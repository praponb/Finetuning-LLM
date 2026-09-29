SELECT * FROM (SELECT id, region, sqft, store_name, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS row_num FROM stores) AS ranked WHERE row_num <= 2
