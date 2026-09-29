SELECT segment, COUNT(*) AS num_customers
FROM customers
WHERE MONTH(signup_date) IN (1, 2, 7) AND segment = 'Corporate'
GROUP BY segment
ORDER BY segment
LIMIT 50
