SELECT MONTH(opened_date) AS opened_month, AVG(sqft) AS avg_sqft, MIN(sqft) AS min_sqft FROM stores GROUP BY MONTH(opened_date) ORDER BY min_sqft;
