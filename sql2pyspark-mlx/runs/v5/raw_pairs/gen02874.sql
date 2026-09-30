WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST, id) AS row_num
    FROM transactions
    QUALIFY row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY amount) AS amount_cume_dist,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LEAD(amount, 1) OVER (PARTITION BY txn_type ORDER BY row_num, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY row_num, id) AS row_seq
    FROM ranked
    WHERE txn_type IN ('DEBIT', 'FEE', 'CREDIT')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(next_amount) OVER w_hist AS last3_sum_next_amount,
        COUNT(next_amount) OVER w_hist AS last3_count_next_amount,
        MIN(next_amount) OVER w_hist AS last3_min_next_amount
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY txn_type, account_id ORDER BY running_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked_transactions AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY next_amount DESC, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.amount,
        src.txn_time,
        src.merchant,
        src.is_flagged,
        src.row_num,
        src.amount_cume_dist,
        src.running_amount,
        src.last3_count_next_amount,
        src.last3_min_next_amount,
        p.account_type
    FROM ranked_transactions src
    INNER JOIN accounts p
        ON src.account_id = p.id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT e.*, a.balance
    FROM joined e
    INNER JOIN accounts a
        ON e.account_id = a.id
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(running_amount) OVER w_recent AS running_min_running_amount,
        MAX(running_amount) OVER w_recent AS running_max_running_amount,
        SUM(running_amount) OVER w_recent AS running_sum_running_amount,
        COUNT(running_amount) OVER w_recent AS running_count_running_amount
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY last3_count_next_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    MAX(txn_time) AS ended_at,
    MAX(amount) FILTER (WHERE merchant = 'Starbucks') AS starbucks_max_amount,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes
FROM with_windows_transactions
GROUP BY DATE_TRUNC('YEAR', txn_time);
