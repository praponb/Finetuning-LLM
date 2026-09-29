SELECT
    id,
    merchant,
    txn_type,
    INITCAP(TRIM(txn_type)) AS txn_type_title
FROM transactions
WHERE HOUR(txn_time) BETWEEN 12 AND 19 AND account_id IN (26, 31);
