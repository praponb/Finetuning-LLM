SELECT *, ROUND(amount * 100, 2) AS amount_adjusted, COALESCE(amount, 0) AS amount_filled FROM transactions WHERE merchant = 'Starbucks' AND HOUR(txn_time) BETWEEN 9 AND 12;
