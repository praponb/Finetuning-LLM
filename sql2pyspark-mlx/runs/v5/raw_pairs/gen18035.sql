SELECT
    id,
    city,
    opened_date,
    sqft,
    SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
    RANK() OVER (ORDER BY opened_date) AS rnk
FROM stores;
