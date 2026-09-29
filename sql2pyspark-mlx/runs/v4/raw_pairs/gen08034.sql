SELECT
    id,
    city,
    country,
    rating,
    DENSE_RANK() OVER (PARTITION BY city, country ORDER BY rating DESC) dense_rnk
FROM suppliers
