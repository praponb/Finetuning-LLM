SELECT
    a.status,
    a.customer_id,
    t.amount,
    t.account_id,
    SUBSTRING(t.merchant, 1, 2) AS merchant_prefix
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
ORDER BY merchant_prefix DESC;
