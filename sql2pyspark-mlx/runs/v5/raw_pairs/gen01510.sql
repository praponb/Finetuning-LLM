WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        SUBSTRING(txn_type, 1, 4) AS txn_type_prefix,
        TRIM(merchant) AS clean_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND NOT is_flagged
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        merchant,
        txn_type_prefix,
        clean_merchant,
        LOWER(merchant) AS merchant_lower
    FROM filtered
    WHERE txn_time >= TO_TIMESTAMP('2023-04-10 20:00:00')
),
filtered_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        merchant,
        txn_type_prefix,
        clean_merchant,
        merchant_lower,
        CASE WHEN merchant IN ('Grab', 'Lazada') THEN 'Priority' ELSE 'Other' END AS merchant_group
    FROM base
    WHERE merchant IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(merchant, '/', txn_type) AS merchant_txn_type
    FROM filtered_transactions
    WHERE txn_type = 'CREDIT'
),
daily AS (
    -- Multi-level aggregation
    SELECT
        merchant,
        SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
        COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count,
        COUNT(*) AS num_rows,
        COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count_2,
        COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count
    FROM valid
    GROUP BY merchant
    HAVING num_txn_types > 1
)
SELECT
    merchant,
    num_rows,
    7_eleven_count,
    CASE WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'OTHER_ZONE' WHEN merchant RLIKE '^(7-Eleven|Lazada)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS merchant_zone
FROM daily
ORDER BY merchant DESC NULLS LAST
LIMIT 20;
