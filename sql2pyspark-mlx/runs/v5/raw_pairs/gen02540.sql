SELECT DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month, SUM(credit_limit) AS total_credit_limit, RANK() OVER (ORDER BY SUM(credit_limit) DESC) AS credit_limit_rank
FROM customers
WHERE segment NOT IN ('Corporate', 'SMB')
GROUP BY DATE_FORMAT(signup_date, 'yyyy-MM')
