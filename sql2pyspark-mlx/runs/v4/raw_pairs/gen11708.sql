SELECT id, account_id, amount, SUM(amount) OVER (PARTITION BY account_id) AS account_id_sum_amount FROM transactions;
