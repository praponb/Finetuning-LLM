SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    NTILE(4) OVER (PARTITION BY region, city ORDER BY sqft, id) AS sqft_bucket,
    AVG(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
    NTILE(10) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket_2,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS row_seq_2
FROM stores
WHERE region IN ('East', 'Central', 'South')
