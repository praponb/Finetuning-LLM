SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    t.amount_count
FROM accounts a
INNER JOIN (SELECT account_id, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_count > 7
ORDER BY a.customer_id DESC, id ASC
LIMIT 3
