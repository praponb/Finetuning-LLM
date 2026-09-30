SELECT city, ROUND((100 * SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_uk, SUM(credit_limit) AS total_credit_limit FROM customers GROUP BY 1;
