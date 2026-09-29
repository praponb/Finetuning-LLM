SELECT is_active, SUM(credit_limit) AS total_credit_limit, ROUND(SUM(credit_limit) * 100.0 / SUM(SUM(credit_limit)) OVER (), 2) AS pct_of_total
FROM customers
GROUP BY is_active
