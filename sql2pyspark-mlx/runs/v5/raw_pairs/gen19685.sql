SELECT
    id,
    opened_date,
    sqft,
    region,
    LEAD(sqft, 1) OVER (ORDER BY opened_date, id) AS next_sqft,
    FIRST_VALUE(opened_date) OVER (PARTITION BY region ORDER BY sqft, id) AS first_opened_date
FROM stores
ORDER BY id DESC;
