SELECT
    id,
    region,
    sqft,
    RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_rank
FROM stores
