SELECT
    c.id,
    c.signup_date,
    c.age,
    t.savings_count,
    t.account_type_values
FROM customers c
INNER JOIN (SELECT customer_id, SUM(CASE WHEN account_type = 'SAVINGS' THEN 1 ELSE 0 END) AS savings_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(account_type))) AS account_type_values
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.savings_count >= 78
ORDER BY id DESC, savings_count ASC
