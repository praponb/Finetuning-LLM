SELECT
    id,
    country,
    rating,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY rating) AS dense_rnk,
    NTILE(4) OVER (PARTITION BY country ORDER BY rating DESC, id) AS quartile
FROM suppliers
WHERE city IN ('London', 'Chiang Mai');
