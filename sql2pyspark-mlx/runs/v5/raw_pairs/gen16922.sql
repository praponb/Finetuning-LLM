WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        SUBSTRING(merchant, 1, 4) merchant_prefix
    FROM transactions
    WHERE merchant IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time DESC, id ASC) = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        d.status,
        d.account_type,
        d.balance
    FROM deduped a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT txn_type, merchant
    FROM enriched
    EXCEPT ALL
    SELECT txn_type, merchant
    FROM transactions
    WHERE NOT is_flagged
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
)
-- Join a dimension and aggregate
SELECT
    d.status AS account_status,
    e.status AS transaction_status,
    COUNT(*) AS num_rows,
    MAX(e.balance) AS max_balance
FROM reconciled e
JOIN accounts d ON e.account_id = d.id
GROUP BY d.status, e.status;
