SELECT
    id,
    country,
    rating,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rn
FROM suppliers
WHERE country IN ('JP', 'UK')
ORDER BY rating DESC, 1;
