WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS row_seq,
        COUNT(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        LAG(amount, 2) OVER (PARTITION BY account_id ORDER BY amount, id) AS prev_amount,
        COUNT(amount) OVER (PARTITION BY merchant) AS merchant_count_amount
    FROM deduped
    WHERE txn_time >= '2025-10-30'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount_count_7d,
        first_known_merchant,
        merchant_count_amount,
        COUNT(amount_count_7d) OVER w_grp AS running_count_amount_count_7d,
        SUM(amount_count_7d) OVER w_grp AS running_sum_amount_count_7d,
        MIN(amount_count_7d) OVER w_grp AS running_min_amount_count_7d,
        MAX(amount_count_7d) OVER w_grp AS running_max_amount_count_7d
    FROM metrics
    WINDOW w_grp AS (PARTITION BY txn_type, account_id ORDER BY merchant_count_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped_transactions AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        txn_type,
        amount_count_7d,
        first_known_merchant,
        running_max_amount_count_7d
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY merchant_count_amount, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
)
SELECT
    id,
    txn_type,
    first_known_merchant,
    CASE WHEN txn_type RLIKE '^(FEE|CREDIT)$' THEN 'PRIORITY' WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'CORE' ELSE 'GROUP_B' END AS txn_type_zone
FROM deduped_transactions
ORDER BY id
LIMIT 20
