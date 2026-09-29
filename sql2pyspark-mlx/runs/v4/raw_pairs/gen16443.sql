SELECT
    id,
    is_flagged,
    account_id,
    UPPER(merchant) merchant_upper
FROM transactions
WHERE txn_time < '2024-06-01'
ORDER BY id DESC, merchant_upper
LIMIT 1;
