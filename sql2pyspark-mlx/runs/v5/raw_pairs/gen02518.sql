SELECT id, txn_time
FROM transactions
WHERE NOT is_flagged AND txn_type <> 'CREDIT' AND merchant IS NOT NULL;
