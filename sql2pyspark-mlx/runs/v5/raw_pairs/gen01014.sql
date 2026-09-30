SELECT region, latest_opened_date, min_sqft
FROM (
    SELECT region, MAX(opened_date) AS latest_opened_date, MIN(sqft) AS min_sqft
    FROM stores
    GROUP BY region
) AS sub
WHERE min_sqft >= 7400;
