WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(amount AS INT) amount_int,
        COALESCE(merchant, 'UNKNOWN') merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2023-04-23'
      AND merchant IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC NULLS FIRST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount) OVER w_running next_amount,
        LEAD(amount_int) OVER w_running next_amount_int,
        ROW_NUMBER() OVER w_running seq_no
    FROM deduped
    WINDOW w_running AS (PARTITION BY account_id, txn_type ORDER BY amount_int, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        merchant,
        next_amount,
        next_amount_int,
        seq_no,
        MAX(seq_no) OVER (PARTITION BY merchant ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_seq_no,
        MIN(seq_no) OVER (PARTITION BY merchant ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_seq_no,
        COUNT(seq_no) OVER (PARTITION BY merchant ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_seq_no
    FROM scored
),
valid_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        merchant,
        next_amount,
        next_amount_int,
        seq_no,
        running_max_seq_no,
        running_count_seq_no,
        CEIL(seq_no) seq_no_whole,
        LEAST(running_count_seq_no, running_min_seq_no) least_running_count_seq_no_running_min_seq_no,
        COALESCE(merchant, 'UNKNOWN') merchant_clean
    FROM with_windows
    WHERE merchant IS NOT NULL
      AND merchant IS NOT DISTINCT FROM 'Grab'
)
SELECT
    id,
    account_id,
    merchant,
    next_amount,
    running_max_seq_no,
    seq_no_whole,
    least_running_count_seq_no_running_min_seq_no,
    CASE WHEN merchant RLIKE '^(Grab|Starbucks)$' THEN 'GROUP_A' ELSE 'CORE' END AS merchant_zone
FROM valid_transactions
ORDER BY running_max_seq_no DESC NULLS LAST, merchant_zone;
