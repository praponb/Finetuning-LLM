SELECT a.id, a.opened_date, t.min_amount
FROM accounts a
LEFT OUTER JOIN (SELECT account_id, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
ORDER BY a.id ASC
