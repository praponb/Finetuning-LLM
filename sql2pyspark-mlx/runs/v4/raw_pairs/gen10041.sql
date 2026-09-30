SELECT
    t.is_flagged,
    t.amount,
    a.customer_id,
    a.opened_date,
    c.city
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.credit_limit BETWEEN 11500 AND 15200
ORDER BY 5 ASC;
