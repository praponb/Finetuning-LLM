SELECT
    t.id,
    t.amount,
    a.customer_id,
    c.is_active,
    c.city
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.txn_type IN ('CREDIT', 'REFUND', 'FEE') AND t.account_id = 13;
