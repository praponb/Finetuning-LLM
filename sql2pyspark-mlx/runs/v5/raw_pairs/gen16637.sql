SELECT id, amount, txn_type
FROM transactions
WHERE is_flagged = FALSE
ORDER BY txn_type DESC, id;
