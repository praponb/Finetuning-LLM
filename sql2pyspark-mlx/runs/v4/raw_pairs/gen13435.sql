SELECT
    city,
    country,
    min_rating,
    th_rating
FROM (
    SELECT
        city,
        country,
        MIN(rating) AS min_rating,
        SUM(CASE WHEN country = 'TH' THEN rating ELSE 0 END) AS th_rating
    FROM suppliers
    GROUP BY city, country
) AS s
WHERE th_rating > 36
