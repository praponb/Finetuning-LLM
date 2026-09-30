SELECT
    id,
    region,
    sqft,
    RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS rnk,
    DENSE_RANK() OVER (ORDER BY sqft) AS dense_rnk
FROM stores;
