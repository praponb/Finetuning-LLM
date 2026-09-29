SELECT id, amount, PERCENT_RANK() OVER (ORDER BY amount DESC) AS amount_pct_rank FROM transactions;
