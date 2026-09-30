SELECT
    t.id,
    t.merchant,
    t.amount,
    a.account_type,
    c.city,
    c.country,
    CEIL(t.amount) AS amount_whole
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
RIGHT JOIN customers c ON a.customer_id = c.id
WHERE NOT t.is_flagged;
