SELECT
    id,
    rating,
    country,
    city
FROM suppliers
WHERE rating > (SELECT MAX(rating) FROM suppliers)
ORDER BY id DESC;
