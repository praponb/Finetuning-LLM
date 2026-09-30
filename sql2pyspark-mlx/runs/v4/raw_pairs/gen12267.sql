SELECT id, merchant, amount, RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS amount_rank FROM transactions;
