SELECT *
FROM (
    SELECT QUARTER(opened_date) opened_quarter, SUM(CASE WHEN city = 'Chiang Mai' THEN sqft ELSE 0 END) chiang_mai_sqft, SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) central_count
    FROM stores
    GROUP BY QUARTER(opened_date)
) x
WHERE chiang_mai_sqft > 77;
