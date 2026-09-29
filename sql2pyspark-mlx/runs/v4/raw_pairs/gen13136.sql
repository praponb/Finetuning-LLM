SELECT *
FROM (
    SELECT QUARTER(opened_date) AS opened_quarter, ROUND(AVG(sqft), 2) AS avg_sqft
    FROM stores
    GROUP BY QUARTER(opened_date)
) AS agg
WHERE avg_sqft >= 77
