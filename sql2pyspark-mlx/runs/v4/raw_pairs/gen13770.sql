SELECT id, opened_date, ROUND(sqft, 0) AS sqft_rounded
FROM stores
WHERE region = 'Central' AND YEAR(opened_date) >= 2023;
