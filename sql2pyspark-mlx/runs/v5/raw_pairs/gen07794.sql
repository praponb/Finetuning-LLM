SELECT
    country,
    rating,
    city,
    ROUND(rating, 0) AS rating_rounded,
    SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix
FROM suppliers
WHERE country IN ('DE', 'JP') AND supplier_name LIKE 'Lio%'
ORDER BY rating_rounded DESC;
