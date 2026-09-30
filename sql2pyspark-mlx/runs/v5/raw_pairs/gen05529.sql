SELECT
    id,
    amount,
    is_flagged,
    txn_type,
    CONCAT_WS('-', merchant, txn_type) AS merchant_txn_type_key
FROM transactions
WHERE is_flagged OR merchant IS NULL;
