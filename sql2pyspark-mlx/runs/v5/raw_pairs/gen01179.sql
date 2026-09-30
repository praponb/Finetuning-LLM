SELECT region, ROUND(AVG(sqft), 2) avg_sqft, MIN(opened_date) min_opened_date FROM stores WHERE opened_date <= '2023-12-14' GROUP BY region;
