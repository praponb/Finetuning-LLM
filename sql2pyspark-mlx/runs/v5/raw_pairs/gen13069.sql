SELECT id, txn_type, ROUND(amount, 2) AS amount_rounded FROM transactions WHERE NOT (account_id IN (14, 26, 35, 40) AND merchant IN ('Amazon', 'Starbucks', 'Shell'))
