SELECT t.id, t.amount, t.account_id, a.account_type FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE t.account_id = 11 AND t.amount BETWEEN 2900 AND 3800
