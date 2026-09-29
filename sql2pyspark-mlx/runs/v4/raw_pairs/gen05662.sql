SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    NTILE(10) OVER (PARTITION BY city, country ORDER BY rating, id) AS rating_bucket,
    SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS seq_no
FROM suppliers
