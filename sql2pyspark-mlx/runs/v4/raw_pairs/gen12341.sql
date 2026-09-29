SELECT
    id,
    account_id,
    is_flagged,
    LENGTH(merchant) AS merchant_length
FROM transactions
WHERE txn_type = 'REFUND' AND txn_time >= '2025-08-01'
