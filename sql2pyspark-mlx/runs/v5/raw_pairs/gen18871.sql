SELECT region, unique_citys, east_sqft
FROM (
    SELECT region, COUNT(DISTINCT sqft) AS unique_citys, SUM(CASE WHEN region = 'East' THEN sqft ELSE 0 END) AS east_sqft
    FROM stores
    WHERE YEAR(opened_date) = 2023
    GROUP BY region
) AS t
WHERE unique_citys < 9;
