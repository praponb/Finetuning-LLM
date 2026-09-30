SELECT region, MAX(opened_date) AS latest_opened_date FROM stores GROUP BY region HAVING AVG(sqft) >= 13100
