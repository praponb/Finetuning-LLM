SELECT
    DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month,
    QUARTER(c.signup_date) AS signup_quarter,
    COUNT(*) AS row_count,
    ROUND(STDDEV(o.discount), 2) AS stddev_discount
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON o.store_id = s.id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM'), QUARTER(c.signup_date)
HAVING COUNT(*) >= 2
ORDER BY 2 DESC, signup_month ASC
LIMIT 5;
