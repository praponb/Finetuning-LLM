SELECT
    id,
    country,
    rating,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) dense_rnk
FROM suppliers
ORDER BY rating DESC;
