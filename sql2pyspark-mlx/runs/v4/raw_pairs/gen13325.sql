SELECT city, ROUND(AVG(age), 2) AS avg_age, MAX(credit_limit) AS max_credit_limit FROM customers WHERE NOT is_active AND name NOT LIKE '%Ford%' GROUP BY city
