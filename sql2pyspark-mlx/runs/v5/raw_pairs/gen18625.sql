SELECT
    id,
    region,
    sqft,
    PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_pct_rank
FROM stores;
