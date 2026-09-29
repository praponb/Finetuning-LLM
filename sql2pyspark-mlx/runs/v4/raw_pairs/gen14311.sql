SELECT
    id,
    opened_date,
    city,
    sqft,
    DENSE_RANK() OVER (ORDER BY opened_date DESC) AS opened_date_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
FROM stores
