SELECT
    id,
    opened_date,
    sqft,
    SUM(sqft) OVER (ORDER BY opened_date, id) AS running_sqft
FROM stores;
