WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        LEAD(amount, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS next_amount,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS LAST) AS amount_dense_rank
    FROM transactions
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS row_seq_2
    FROM enriched
    WHERE amount_dense_rank < 7
    WINDOW w_ordered AS (ORDER BY amount_dense_rank, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        merchant,
        row_seq,
        first_known_amount,
        MAX(row_seq) OVER w_grp AS running_max_row_seq,
        COUNT(row_seq) OVER w_grp AS running_count_row_seq,
        SUM(row_seq) OVER w_grp AS running_sum_row_seq,
        LEAD(amount_dense_rank, 1) OVER (PARTITION BY txn_type, merchant ORDER BY row_seq_2, id) AS next_amount_dense_rank
    FROM enriched_transactions
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY first_known_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        amount,
        first_known_amount,
        running_max_row_seq,
        running_sum_row_seq,
        next_amount_dense_rank
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        running_max_row_seq,
        running_sum_row_seq,
        next_amount_dense_rank,
        CAST(running_max_row_seq AS INT) AS running_max_row_seq_int
    FROM ranked
    WHERE next_amount_dense_rank <= 5
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_max_row_seq >= 9.0 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_row_seq_tier,
        CASE WHEN running_sum_row_seq >= 34 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_row_seq_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(running_max_row_seq) AS max_running_max_row_seq,
        ROUND(AVG(running_sum_row_seq), 2) AS avg_running_sum_row_seq
    FROM base
    GROUP BY CASE WHEN running_max_row_seq >= 9.0 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_sum_row_seq >= 34 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING COUNT(DISTINCT id) > 1
)
SELECT
    running_max_row_seq_tier,
    running_sum_row_seq_tier,
    max_running_max_row_seq,
    avg_running_sum_row_seq
FROM grouped
ORDER BY max_running_max_row_seq DESC NULLS FIRST
