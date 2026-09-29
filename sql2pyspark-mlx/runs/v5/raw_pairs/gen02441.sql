SELECT ROUND((100 * SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_corporate, AVG(credit_limit) AS avg_credit_limit FROM customers;
