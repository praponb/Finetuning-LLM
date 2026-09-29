WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', txn_type, merchant) txn_type_merchant_key,
        INITCAP(TRIM(merchant)) merchant_title,
        COALESCE(merchant, 'OTHER') merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
      AND TO_DATE(txn_time) >= '2025-10-01'
)
-- Attach accounts attributes
SELECT
    src.id,
    src.account_id,
    src.amount,
    src.txn_time,
    src.merchant,
    src.merchant_clean,
    a.status,
    a.account_type
FROM base src
LEFT JOIN accounts a
    ON src.account_id = a.id
WHERE a.customer_id = 1
