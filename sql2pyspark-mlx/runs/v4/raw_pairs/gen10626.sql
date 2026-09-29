SELECT id, account_id, amount, AVG(amount) OVER (PARTITION BY account_id) AS account_id_avg_amount FROM transactions
