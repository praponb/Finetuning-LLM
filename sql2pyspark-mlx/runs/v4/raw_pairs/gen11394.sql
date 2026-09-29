SELECT
    id,
    city,
    country,
    rating
FROM suppliers
WHERE country = 'TH' AND country IN ('TH', 'SG', 'US')
ORDER BY rating;
