WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        CONCAT_WS(',', merchant, txn_type) merchant_txn_type_key
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Amazon'
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        d.account_type,
        d.balance,
        d.opened_date
    FROM base src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        is_flagged,
        merchant_txn_type_key,
        LEAD(amount, 2) OVER (PARTITION BY account_type ORDER BY balance, id) next_amount,
        AVG(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_90d,
        LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY opened_date, id) prev_amount
    FROM with_dims
    WHERE TO_DATE(txn_time) >= '2025-05-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        merchant_txn_type_key,
        LAG(next_amount) OVER (PARTITION BY account_id ORDER BY prev_amount, id) prev_next_amount,
        LEAD(amount_avg_90d) OVER (PARTITION BY account_id ORDER BY prev_amount, id) next_amount_avg_90d,
        LAG(amount_avg_90d) OVER (PARTITION BY account_id ORDER BY prev_amount, id) prev_amount_avg_90d,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY prev_amount, id) event_seq,
        COUNT(prev_amount) OVER (PARTITION BY account_id) account_id_count_prev_amount
    FROM scored
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        merchant_txn_type_key,
        prev_next_amount,
        prev_amount_avg_90d,
        account_id_count_prev_amount,
        CEIL(prev_next_amount) prev_next_amount_whole
    FROM with_windows
    WHERE next_amount_avg_90d < 1359
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(prev_next_amount_whole) OVER (ORDER BY account_id_count_prev_amount, id) prev_prev_next_amount_whole,
        LEAD(prev_amount_avg_90d) OVER (ORDER BY account_id_count_prev_amount, id) next_prev_amount_avg_90d,
        ROW_NUMBER() OVER (ORDER BY account_id_count_prev_amount, id) row_seq,
        FIRST_VALUE(prev_next_amount, TRUE) OVER (ORDER BY prev_next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_prev_next_amount
    FROM valid
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric_name,
    metric_value
FROM (
    SELECT
        id,
        prev_next_amount,
        first_known_prev_next_amount
    FROM with_windows_transactions
) src
UNPIVOT (
    metric_value FOR metric_name IN (prev_next_amount, first_known_prev_next_amount)
)
