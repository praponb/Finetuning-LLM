SELECT
    c.id,
    c.signup_date,
    t.min_balance,
    t.status_list
FROM customers c
LEFT JOIN (SELECT customer_id, MIN(balance) AS min_balance, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.min_balance > 55000;
