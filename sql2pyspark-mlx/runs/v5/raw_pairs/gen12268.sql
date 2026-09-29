SELECT txn_time, amount, CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END AS is_lazada FROM transactions WHERE merchant IN ('Lazada', '7-Eleven', 'Shell') AND amount < 2500;
