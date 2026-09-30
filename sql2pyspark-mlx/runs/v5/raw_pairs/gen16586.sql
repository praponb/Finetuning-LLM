SELECT
    id,
    is_flagged,
    amount,
    LOWER(txn_type) AS txn_type_lower
FROM transactions
WHERE amount < 3200 OR txn_type != 'CREDIT';
