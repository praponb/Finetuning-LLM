SELECT
    id,
    country,
    rating,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rating_row
FROM suppliers
WHERE rating > 2.5;
