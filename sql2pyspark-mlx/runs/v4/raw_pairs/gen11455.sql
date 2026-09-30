WITH numbered AS (SELECT *, RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS row_num FROM stores WHERE store_name LIKE 'Ol%') SELECT * FROM numbered WHERE row_num <= 3;
