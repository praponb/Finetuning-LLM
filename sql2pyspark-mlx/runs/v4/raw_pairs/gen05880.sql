SELECT
    c.is_active,
    YEAR(c.signup_date) AS signup_year,
    SORT_ARRAY(COLLECT_SET(a.status)) AS status_list,
    COUNT(DISTINCT a.balance) AS unique_statuss
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY c.is_active, YEAR(c.signup_date)
HAVING COUNT(DISTINCT a.balance) >= 5
ORDER BY signup_year DESC;
