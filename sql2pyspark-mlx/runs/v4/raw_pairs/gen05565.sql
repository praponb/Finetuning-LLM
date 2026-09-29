WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS row_num
        FROM transactions
    ) t
    WHERE row_num = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        row_num,
        CAST(is_flagged AS INT) is_flagged_flag,
        COALESCE(merchant, 'NONE') merchant_filled
    FROM deduped
    WHERE txn_time >= TIMESTAMP '2024-05-30 03:00:00'
      AND is_flagged
),
valid_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(is_flagged AS INT) is_flagged_flag_2
    FROM valid
    WHERE txn_time >= TIMESTAMP '2024-02-18 14:00:00'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(is_flagged_flag_2 / row_num, 2) is_flagged_flag_2_per_row_num,
        CAST(is_flagged AS INT) is_flagged_flag_3
    FROM valid_transactions
    WHERE is_flagged_flag_2 BETWEEN 57 AND 62
),
joined AS (
    -- Attach accounts attributes
    SELECT a.*, d.status
    FROM filtered a
    INNER JOIN accounts d
        ON a.account_id = d.id
)
-- Running and moving aggregates
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    row_num,
    is_flagged_flag,
    merchant_filled,
    is_flagged_flag_2,
    is_flagged_flag_2_per_row_num,
    is_flagged_flag_3,
    status,
    FIRST_VALUE(is_flagged_flag_3, TRUE) OVER (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_is_flagged_flag_3,
    SUM(is_flagged_flag_2) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_flagged_flag_2,
    AVG(is_flagged_flag) OVER (PARTITION BY merchant, status) merchant_avg_is_flagged_flag
FROM joined
