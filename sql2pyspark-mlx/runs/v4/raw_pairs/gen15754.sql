SELECT
    c.id,
    c.city,
    t.checking_count,
    t.account_type_list
FROM customers c
JOIN (SELECT customer_id, SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END) AS checking_count, SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.checking_count > 22
ORDER BY checking_count DESC;
