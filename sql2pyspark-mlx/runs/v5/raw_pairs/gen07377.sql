WITH RecentStores AS (
    SELECT *
    FROM stores
    WHERE city = 'Tokyo' AND sqft >= 7977 AND city = 'Chiang Mai'
),
agg AS (
    SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, MIN(opened_date) first_opened_date
    FROM RecentStores
    GROUP BY CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT opened_cohort, first_opened_date
FROM agg;
