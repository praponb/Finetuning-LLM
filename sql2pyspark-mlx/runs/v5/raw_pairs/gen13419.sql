SELECT id, region, opened_date, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date) AS opened_date_dense_rank FROM stores;
