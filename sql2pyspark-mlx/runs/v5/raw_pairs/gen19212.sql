SELECT
    YEAR(c.signup_date) AS signup_year,
    c.is_active,
    MIN(a.balance) AS min_balance,
    SORT_ARRAY(COLLECT_SET(a.status)) AS status_list
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE c.city != 'Bangkok'
GROUP BY YEAR(c.signup_date), c.is_active
ORDER BY signup_year ASC, min_balance ASC, is_active ASC
LIMIT 5;
