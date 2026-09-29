SELECT id, account_id, amount, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS amount_row FROM transactions;
