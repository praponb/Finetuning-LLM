WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank,
        NTILE(10) OVER (ORDER BY amount, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY txn_type, merchant) AS txn_type_sum_amount,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM transactions
    WINDOW w_ordered AS (PARTITION BY merchant ORDER BY txn_time, id)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_type_sum_amount, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        amount_percent_rank,
        amount_bucket,
        txn_type_sum_amount,
        row_num,
        COALESCE(amount_percent_rank, 0) AS amount_percent_rank_filled,
        CONCAT_WS('/', merchant, txn_type) AS merchant_txn_type_key
    FROM ranked
    WHERE merchant IS DISTINCT FROM 'Grab'
      AND txn_time >= TO_TIMESTAMP('2023-10-15 12:00:00')
),
ranked_transactions AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        amount_bucket,
        merchant_txn_type_key
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY amount_percent_rank_filled DESC NULLS FIRST, id) = 1
),
ranked_transactions_2 AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST, id) AS rn
        FROM ranked_transactions
    ) ranked
    WHERE rn = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list_2,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95,
    COUNT_IF(txn_type = 'DEBIT') AS debit_events,
    PERCENTILE_APPROX(amount_bucket, ARRAY(0.25, 0.5, 0.75)) AS amount_bucket_quartiles,
    COUNT(*) AS cnt
FROM ranked_transactions_2
GROUP BY 1, 2
HAVING debit_events < 1000
ORDER BY amount_p95 DESC NULLS LAST;
