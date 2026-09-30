SELECT id, credit_limit, city, CASE WHEN age >= 71 THEN 'High' WHEN age >= 30 THEN 'Medium' ELSE 'Low' END AS age_band FROM customers WHERE segment = 'Consumer' ORDER BY 3 ASC;
