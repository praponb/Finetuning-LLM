SELECT country, email
FROM customers
WHERE signup_date BETWEEN '2023-06-08' AND '2024-12-17'
UNION ALL
SELECT country, email
FROM customers
WHERE country NOT IN ('UK', 'TH', 'JP')
ORDER BY country DESC
