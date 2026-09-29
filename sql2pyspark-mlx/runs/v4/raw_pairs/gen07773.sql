SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions WHERE account_id IN (2, 12, 36, 37) AND (amount <= 1900 OR txn_type NOT IN ('FEE', 'CREDIT')) ORDER BY merchant, id LIMIT 1;
