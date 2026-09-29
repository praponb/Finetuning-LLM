WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        TRIM(merchant) AS clean_merchant,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2024-04-19 15:00:00'
      AND txn_type IN ('REFUND', 'FEE')
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.status,
        p.opened_date
    FROM filtered e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
filtered_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        resolved_merchant,
        SUBSTRING(txn_type, 1, 4) AS txn_type_prefix
    FROM with_dims
    WHERE txn_time > DATE '2023-11-20'
      AND status IS NOT NULL
      AND txn_type = 'REFUND'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time NULLS FIRST, id) = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        resolved_merchant,
        txn_type_prefix,
        CASE WHEN amount >= 3500 THEN 'High' WHEN amount >= 1300 THEN 'Medium' ELSE 'Low' END AS amount_tier
    FROM deduped
    WHERE txn_time >= DATE '2024-05-24'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    id,
    txn_type,
    amount,
    merchant,
    txn_type_prefix,
    amount_tier,
    session_start_flag,
    island_id,
    CASE WHEN merchant RLIKE '^(Starbucks|Lazada)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM sessionized;
