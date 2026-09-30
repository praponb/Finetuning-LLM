WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CONCAT_WS(',', merchant, txn_type) AS merchant_txn_type_key,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT merchant, txn_type
    FROM base
    EXCEPT DISTINCT
    SELECT merchant, txn_type
    FROM transactions
    WHERE amount < 3200
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.merchant = k.merchant AND s.txn_type = k.txn_type
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(amount, 0) AS amount_rounded
    FROM reconciled
    WHERE is_flagged = TRUE
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.account_id,
        e.txn_type,
        e.amount,
        e.txn_time,
        e.merchant_txn_type_key,
        e.amount_rounded,
        a.account_type,
        a.status,
        a.opened_date
    FROM valid e
    LEFT JOIN accounts a
        ON e.account_id = a.id
)
-- Join a dimension and aggregate
SELECT
    p.status AS account_status,
    p.account_type AS account_account_type,
    src.account_type AS transaction_account_type,
    COUNT(*) AS rows_total,
    MAX(src.amount) AS max_amount,
    GROUPING_ID() AS gid
FROM with_dims src
INNER JOIN accounts p ON src.account_id = p.id
GROUP BY GROUPING SETS ((p.status, p.account_type, src.account_type), (p.status))
ORDER BY account_status DESC NULLS FIRST
