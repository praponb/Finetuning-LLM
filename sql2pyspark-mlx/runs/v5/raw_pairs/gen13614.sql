WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(merchant) AS clean_merchant,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE amount > 1400
      AND txn_time > DATE '2023-05-08'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        clean_merchant
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time DESC NULLS LAST, id) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
base_transactions AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(merchant, 'N/A') AS merchant_filled
    FROM latest
    WHERE account_id IN (18, 20)
),
base_transactions_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        clean_merchant,
        CONCAT(txn_type, ', ', merchant) AS txn_type_merchant,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM base_transactions
    WHERE account_id IN (3, 10, 13, 20)
)
-- Aggregate with conditional metrics
SELECT
    merchant,
    account_id,
    COUNT(*) AS row_count,
    COUNT_IF(merchant = 'Starbucks') AS starbucks_events,
    COUNT(DISTINCT merchant) AS distinct_merchants,
    COUNT(DISTINCT txn_type) AS distinct_txn_types
FROM base_transactions_2
GROUP BY 1, 2
ORDER BY distinct_merchants DESC
