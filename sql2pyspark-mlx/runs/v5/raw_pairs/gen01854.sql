WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY merchant, txn_type ORDER BY amount NULLS LAST) AS amount_percent_rank,
        NTILE(4) OVER (PARTITION BY txn_type, merchant ORDER BY amount, id) AS amount_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM transactions
    WINDOW w_running AS (PARTITION BY merchant, txn_type ORDER BY amount, id)
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.txn_time,
        s.merchant,
        s.is_flagged,
        s.amount_percent_rank,
        s.seq_no,
        a.account_type,
        a.balance
    FROM enriched s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY txn_type ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_seq_no,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY txn_type), 4) AS seq_no_share,
        FIRST_VALUE(seq_no) IGNORE NULLS OVER w_hist AS first_known_seq_no,
        NTILE(10) OVER (ORDER BY amount_percent_rank, id) AS amount_percent_rank_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY amount_percent_rank DESC, id) AS row_seq
    FROM enriched_transactions
    WINDOW w_hist AS (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_time,
        amount_percent_rank,
        account_type,
        running_seq_no,
        MIN(running_seq_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_running_seq_no,
        MAX(running_seq_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_running_seq_no,
        AVG(running_seq_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_running_seq_no,
        SUM(running_seq_no) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_running_seq_no,
        NTILE(3) OVER (PARTITION BY account_type ORDER BY running_seq_no DESC, id) AS running_seq_no_bucket
    FROM scored
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY amount_percent_rank DESC NULLS LAST, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(running_seq_no, last7_sum_running_seq_no) AS greatest_running_seq_no_last7_sum_running_seq_no,
        LOWER(account_type) AS account_type_lower,
        COALESCE(account_type, 'UNKNOWN') AS resolved_account_type
    FROM latest
    WHERE txn_time > TIMESTAMP '2023-05-03 12:00:00'
      AND account_type IN ('SAVINGS', 'LOAN', 'CHECKING')
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('MONTH', txn_time) AS activity_month,
        CASE WHEN running_seq_no >= 45 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_seq_no_tier,
        SUM(CASE WHEN account_type = 'CHECKING' THEN running_seq_no ELSE 0 END) AS checking_running_seq_no,
        COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END) AS credit_count,
        (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
        COUNT(*) AS cnt,
        GROUPING_ID() AS grouping_id
    FROM base
    GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', txn_time), CASE WHEN running_seq_no >= 45 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('MONTH', txn_time)), ())
    HAVING COUNT(*) >= 1
)
SELECT
    checking_running_seq_no,
    span_minutes,
    cnt
FROM stats
ORDER BY checking_running_seq_no DESC
