SELECT region, SUM(sqft) AS total_sqft, MAX(sqft) AS max_sqft FROM stores WHERE opened_date BETWEEN '2025-01-01' AND '2025-10-07' AND sqft = 15406 GROUP BY 1;
