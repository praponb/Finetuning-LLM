SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity) AS row_num FROM inventory) AS r WHERE row_num <= 3 ORDER BY 5 ASC
