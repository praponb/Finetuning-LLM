SELECT txn_type, is_flagged
FROM transactions
WHERE txn_type IN ('DEBIT', 'CREDIT')
ORDER BY txn_type;
