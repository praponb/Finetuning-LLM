SELECT
    id,
    txn_type,
    LOWER(merchant) AS merchant_lower,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE account_id IN (17, 28, 32)
ORDER BY merchant_lower;
