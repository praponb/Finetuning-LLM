SELECT
    t.id,
    t.amount,
    t.account_id,
    t.merchant,
    a.status
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.account_id IN (1, 18, 23) AND a.customer_id IS NULL;
