WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(merchant, ' ', txn_type) merchant_txn_type
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-02-18 23:00:00'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        dedup_rank * amount dedup_rank_times_amount,
        CEIL(amount) amount_whole,
        COALESCE(merchant, 'NONE') merchant_clean
    FROM deduped
    WHERE txn_time > TIMESTAMP '2024-03-17 20:00:00'
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    merchant_txn_type,
    dedup_rank,
    dedup_rank_times_amount,
    CASE WHEN txn_type RLIKE '^(DEBIT|FEE)$' THEN 'EXTENDED' WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone
FROM cleaned
