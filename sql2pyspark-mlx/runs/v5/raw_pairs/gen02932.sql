SELECT
    QUARTER(signup_date) AS signup_quarter,
    SUM(credit_limit) AS total_credit_limit,
    COUNT(*) AS cnt,
    ROUND(SUM(credit_limit) * 100.0 / SUM(SUM(credit_limit)) OVER (), 2) AS pct_of_total
FROM customers
GROUP BY QUARTER(signup_date)
ORDER BY signup_quarter DESC;
