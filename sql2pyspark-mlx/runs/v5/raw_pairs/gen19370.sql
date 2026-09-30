SELECT
    id,
    city,
    rating,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS dense_rnk
FROM suppliers
ORDER BY 3, dense_rnk DESC;
