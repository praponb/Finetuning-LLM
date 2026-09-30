WITH src AS (
    SELECT *, LENGTH(city) AS city_length
    FROM stores
    WHERE region IN ('West', 'East', 'South') AND opened_date BETWEEN '2024-08-01' AND '2025-04-01' AND region IN ('North', 'West', 'East')
),
opened_year_summary AS (
    SELECT YEAR(opened_date) AS opened_year, COUNT(DISTINCT city_length) AS unique_regions
    FROM src
    GROUP BY YEAR(opened_date)
)
SELECT opened_year, unique_regions
FROM opened_year_summary;
