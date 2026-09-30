SELECT
    a.balance,
    t.merchant,
    t.is_flagged,
    COALESCE(a.balance, 0) AS balance_filled
FROM accounts a
JOIN transactions t ON a.id = t.account_id
WHERE a.status NOT IN ('OPEN', 'FROZEN')
ORDER BY balance_filled DESC
