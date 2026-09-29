SELECT YEAR(opened_date) AS opened_year, MIN(sqft) AS min_sqft, MIN(opened_date) AS min_opened_date FROM stores GROUP BY YEAR(opened_date);
