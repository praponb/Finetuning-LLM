SELECT
    t.id,
    t.account_id,
    a.opened_date,
    c.age
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON c.id = a.customer_id
WHERE c.city = 'Chiang Mai' AND a.opened_date >= '2025-03-30';
