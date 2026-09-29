SELECT MONTH(opened_date) AS opened_month, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
GROUP BY opened_month;
