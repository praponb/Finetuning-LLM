WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Starbucks'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    MAX(txn_time) AS ended_at
FROM cleaned
GROUP BY DATE_TRUNC('QUARTER', txn_time);
