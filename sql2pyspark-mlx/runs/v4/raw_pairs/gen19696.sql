SELECT
    id,
    rating,
    country,
    supplier_name,
    CAST(rating AS INT) AS rating_int
FROM suppliers
