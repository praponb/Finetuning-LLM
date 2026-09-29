SELECT
    id,
    city,
    country,
    rating,
    SUBSTRING(supplier_name, 1, 4) AS supplier_name_prefix,
    COALESCE(rating, 0) AS rating_filled
FROM suppliers
ORDER BY id DESC;
