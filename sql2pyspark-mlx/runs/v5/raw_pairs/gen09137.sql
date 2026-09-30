WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS amount_dense_rank,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d
    FROM transactions
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        INITCAP(TRIM(txn_type)) AS txn_type_title
    FROM enriched
    WHERE txn_time >= TO_DATE('2023-06-27')
      AND is_flagged = FALSE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(merchant) AS merchant_lower
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND is_flagged
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS FIRST) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
deduped_transactions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY dedup_rank DESC, id) AS latest_rank
        FROM deduped
    ) t
    WHERE latest_rank = 1
)
-- Ranking and gap features
SELECT
    id,
    txn_time,
    is_flagged,
    latest_rank,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY latest_rank, id) AS seq_no,
    LAST_VALUE(merchant, TRUE) OVER (PARTITION BY account_id, merchant ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
    ROW_NUMBER() OVER (ORDER BY txn_time, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS event_seq
FROM deduped_transactions;
