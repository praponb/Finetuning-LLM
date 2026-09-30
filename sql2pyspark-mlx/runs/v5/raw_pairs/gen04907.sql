SELECT SUM(sqft) AS total_sqft FROM stores WHERE city = 'Berlin' AND (opened_date <= '2025-01-01' OR region IN ('South', 'North', 'West')) ORDER BY total_sqft DESC;
