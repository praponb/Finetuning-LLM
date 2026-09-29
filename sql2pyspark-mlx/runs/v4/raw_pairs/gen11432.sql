SELECT account_id FROM transactions WHERE merchant IN ('Shell', 'Lazada') UNION ALL SELECT account_id FROM transactions WHERE amount <= 1800 ORDER BY account_id;
