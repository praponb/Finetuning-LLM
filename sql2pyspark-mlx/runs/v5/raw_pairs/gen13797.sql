WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        is_flagged,
        DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label,
        ROUND(amount, 0) AS amount_rounded
    FROM transactions
    WHERE merchant IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        amount,
        txn_time_label,
        amount_rounded,
        LAG(amount) OVER (PARTITION BY txn_type ORDER BY amount_rounded, id) AS prev_amount,
        LAG(amount_rounded) OVER (PARTITION BY txn_type ORDER BY amount_rounded, id) AS prev_amount_rounded,
        LEAD(amount_rounded) OVER (PARTITION BY txn_type ORDER BY amount_rounded, id) AS next_amount_rounded,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount_rounded, id) AS event_seq
    FROM base
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(prev_amount_rounded / amount, 2) AS prev_amount_rounded_per_amount,
        ROUND(prev_amount * 0.9, 2) AS prev_amount_adjusted
    FROM enriched
    WHERE prev_amount BETWEEN 2200 AND 2300
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(amount, prev_amount_rounded) AS greatest_amount_prev_amount_rounded
    FROM cleaned
    WHERE prev_amount_adjusted < 63
),
cleaned_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        amount,
        amount_rounded,
        prev_amount,
        next_amount_rounded,
        prev_amount_adjusted,
        greatest_amount_prev_amount_rounded,
        prev_amount_rounded * prev_amount_rounded_per_amount AS prev_amount_rounded_times_prev_amount_rounded_per_amount,
        TRIM(txn_type) AS clean_txn_type
    FROM filtered
    WHERE prev_amount_adjusted > 21
),
enriched_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(next_amount_rounded) OVER w_recent AS running_max_next_amount_rounded,
        MIN(next_amount_rounded) OVER w_recent AS running_min_next_amount_rounded,
        COUNT(next_amount_rounded) OVER w_recent AS running_count_next_amount_rounded
    FROM cleaned_transactions
    WHERE greatest_amount_prev_amount_rounded > 38
    WINDOW w_recent AS (ORDER BY prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_transactions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(prev_amount_rounded_times_prev_amount_rounded_per_amount) OVER w_hist AS next_prev_amount_rounded_times_prev_amount_rounded_per_amount,
        LEAD(amount) OVER w_hist AS next_amount,
        LAG(amount) OVER w_hist AS prev_amount_2,
        ROW_NUMBER() OVER w_hist AS row_seq,
        FIRST_VALUE(prev_amount) IGNORE NULLS OVER (ORDER BY running_count_next_amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_amount
    FROM enriched_transactions
    WINDOW w_hist AS (ORDER BY prev_amount_adjusted, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_min_next_amount_rounded >= 61 THEN 'HIGH' ELSE 'LOW' END AS running_min_next_amount_rounded_tier,
    CASE WHEN amount >= 4100 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    CASE WHEN prev_amount_rounded_times_prev_amount_rounded_per_amount >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_amount_rounded_times_prev_amount_rounded_per_amount_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(row_seq) AS max_row_seq,
    SUM(running_count_next_amount_rounded) AS total_running_count_next_amount_rounded
FROM enriched_transactions_2
GROUP BY CASE WHEN running_min_next_amount_rounded >= 61 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN amount >= 4100 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_amount_rounded_times_prev_amount_rounded_per_amount >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END;
