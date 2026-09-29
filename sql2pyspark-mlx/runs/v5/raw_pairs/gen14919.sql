WITH FilteredStores AS (
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM stores
    WHERE opened_date < '2024-09-01' AND (opened_date < '2025-10-14' OR sqft < 5390)
),
summary AS (
    SELECT QUARTER(opened_date) AS opened_quarter, MIN(opened_date) AS min_opened_date, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count
    FROM FilteredStores
    GROUP BY QUARTER(opened_date)
)
SELECT *
FROM summary
WHERE singapore_count >= 73
ORDER BY min_opened_date;
