WITH ranked AS (SELECT id, region, opened_date, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS row_num FROM stores) SELECT id, region, opened_date FROM ranked WHERE row_num <= 3;
