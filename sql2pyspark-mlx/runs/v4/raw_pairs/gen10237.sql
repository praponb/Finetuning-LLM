SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag, SUBSTRING(txn_type, 1, 2) AS txn_type_prefix FROM transactions WHERE txn_time >= '2025-09-20' AND (merchant = 'Lazada' OR amount < 1400)
