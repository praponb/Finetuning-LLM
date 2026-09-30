SELECT id, store_id, units FROM (SELECT id, store_id, units, RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS row_num FROM store_sales) AS t WHERE row_num <= 3
