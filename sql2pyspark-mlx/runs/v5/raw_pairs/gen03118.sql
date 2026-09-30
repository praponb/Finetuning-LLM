SELECT
    t.id,
    t.merchant,
    a.status,
    a.customer_id,
    c.email,
    c.age
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE c.name LIKE 'J%'
ORDER BY merchant;
