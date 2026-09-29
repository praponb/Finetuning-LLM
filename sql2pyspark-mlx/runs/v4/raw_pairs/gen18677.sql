WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        UPPER(merchant) merchant_upper
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2024-02-22 06:00:00')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_upper,
        COALESCE(merchant, 'N/A') merchant_filled,
        UPPER(merchant) merchant_upper_2
    FROM cleaned
    WHERE txn_time > TO_DATE('2024-02-29')
      AND merchant IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS FIRST, id ASC) AS rn
        FROM valid
    ) t
    WHERE rn = 1
)
SELECT
    id,
    account_id,
    txn_time,
    merchant,
    is_flagged,
    merchant_upper,
    merchant_filled,
    merchant_upper_2
FROM latest
ORDER BY merchant DESC;
