SELECT
    id,
    region,
    sqft,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft) dense_rnk
FROM stores
ORDER BY 4 DESC, sqft DESC;
