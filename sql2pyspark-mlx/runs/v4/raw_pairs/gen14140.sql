SELECT
    a.id,
    a.balance,
    a.account_type,
    t.min_amount
FROM accounts a
INNER JOIN (SELECT account_id, MIN(amount) min_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
