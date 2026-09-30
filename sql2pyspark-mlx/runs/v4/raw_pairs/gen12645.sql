SELECT * FROM (SELECT id, store_id, amount, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC, id) AS row_num FROM orders) AS r WHERE row_num <= 2;
