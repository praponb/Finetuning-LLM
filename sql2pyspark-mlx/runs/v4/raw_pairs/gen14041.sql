SELECT *, NTILE(3) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket, LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_sqft FROM stores
