SELECT *, TRIM(region) AS clean_region, ROUND(sqft * 1.07, 2) AS adjusted_sqft
FROM stores
WHERE city = 'Berlin' AND opened_date < '2025-09-25'
