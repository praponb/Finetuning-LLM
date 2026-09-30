SELECT
    id,
    sqft,
    store_name,
    region,
    opened_date,
    LAST_VALUE(store_name) OVER (ORDER BY sqft DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_name,
    RANK() OVER (PARTITION BY region ORDER BY opened_date) AS rnk
FROM stores
WHERE store_name LIKE '%Flagshi%';
