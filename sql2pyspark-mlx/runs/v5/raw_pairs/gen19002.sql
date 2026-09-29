SELECT
    a.customer_id,
    a.opened_date,
    t.is_flagged,
    t.txn_type,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE t.txn_type = 'CREDIT'
