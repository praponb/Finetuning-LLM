SELECT
    id,
    city,
    rating,
    country,
    LAST_VALUE(country) OVER (PARTITION BY city ORDER BY rating DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_country
FROM suppliers
WHERE rating > 2.8
