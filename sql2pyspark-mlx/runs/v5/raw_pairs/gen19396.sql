SELECT
    t.id,
    t.merchant,
    t.is_flagged,
    a.opened_date,
    a.status,
    c.city,
    c.country
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT OUTER JOIN customers c ON a.customer_id = c.id
WHERE a.account_type = 'CHECKING' AND a.account_type IN ('CHECKING', 'LOAN', 'CREDIT')
ORDER BY t.id DESC, opened_date;
