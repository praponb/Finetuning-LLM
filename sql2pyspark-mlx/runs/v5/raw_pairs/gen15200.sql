SELECT t.id, t.merchant, a.opened_date
FROM transactions t
RIGHT JOIN accounts a ON t.account_id = a.id
