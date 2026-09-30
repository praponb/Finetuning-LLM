SELECT is_active, ROUND(AVG(credit_limit), 2) AS avg_credit_limit FROM customers WHERE NOT is_active GROUP BY 1 ORDER BY avg_credit_limit DESC;
