SELECT id, account_id, amount, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS rn FROM transactions WHERE txn_time >= '2023-12-01';
