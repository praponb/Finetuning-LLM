SELECT
    id,
    region,
    opened_date,
    city,
    LAST_VALUE(city) OVER (PARTITION BY region ORDER BY opened_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_city
FROM stores
WHERE opened_date >= '2023-11-01';
