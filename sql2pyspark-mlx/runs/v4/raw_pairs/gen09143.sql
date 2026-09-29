SELECT id, region, sqft, DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_dense_rank FROM stores;
