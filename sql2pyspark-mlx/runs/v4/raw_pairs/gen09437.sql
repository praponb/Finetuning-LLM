SELECT
    QUARTER(opened_date) AS opened_quarter,
    city,
    MAX(opened_date) AS last_opened_date,
    COUNT(sqft) AS sqft_count
FROM stores
GROUP BY QUARTER(opened_date), city
HAVING AVG(sqft) < 11000
ORDER BY sqft_count
