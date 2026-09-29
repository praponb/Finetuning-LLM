SELECT
    id,
    opened_date,
    sqft,
    region,
    FIRST_VALUE(sqft) OVER (ORDER BY opened_date, id) first_sqft,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) dense_rnk
FROM stores
WHERE store_name LIKE '%Downto%'
