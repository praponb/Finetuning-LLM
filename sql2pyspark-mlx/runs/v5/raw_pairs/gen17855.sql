SELECT
    id,
    supplier_name,
    city,
    rating,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST) AS rating_dense_rank,
    ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
    NTILE(5) OVER (PARTITION BY country, city ORDER BY rating DESC, id) AS rating_bucket
FROM suppliers
