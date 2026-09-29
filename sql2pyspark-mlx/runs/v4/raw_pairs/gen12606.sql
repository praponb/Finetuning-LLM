WITH src AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name NOT LIKE 'Gl%' AND country = 'US'
),
CountrySummary AS (
    SELECT country, MAX(rating) - MIN(rating) AS rating_range
    FROM src
    GROUP BY country
)
SELECT country, rating_range
FROM CountrySummary
WHERE rating_range >= 76;
