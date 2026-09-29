SELECT
    c.id,
    c.city,
    c.signup_date,
    t.checking_count,
    t.avg_balance
FROM customers c
LEFT OUTER JOIN (SELECT customer_id, SUM(CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END) checking_count, ROUND(AVG(balance), 2) avg_balance
    FROM accounts
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.checking_count >= 73
ORDER BY c.signup_date DESC;
