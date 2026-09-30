WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER w_hist AS balance_bucket,
        LEAD(balance, 2) OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC) AS next_balance
    FROM accounts
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY balance ASC, id ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        status,
        balance_bucket,
        AVG(next_balance) OVER (PARTITION BY account_type, status ORDER BY balance_bucket ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_balance_avg_5,
        SUM(next_balance) OVER (PARTITION BY customer_id ORDER BY balance_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_balance,
        AVG(next_balance) OVER (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_balance_avg_5_2,
        NTILE(4) OVER (ORDER BY next_balance ASC, id ASC) AS next_balance_bucket
    FROM scored
    WHERE account_type IN ('SAVINGS', 'CREDIT')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(next_balance_bucket) OVER (PARTITION BY status) AS status_sum_next_balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_bucket ASC, id ASC) AS seq_no,
        SUM(next_balance_avg_5) OVER (ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_balance_avg_5,
        NTILE(4) OVER (PARTITION BY status, account_type ORDER BY next_balance_bucket ASC, id ASC) AS next_balance_bucket_bucket
    FROM with_windows
),
metrics_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY next_balance_avg_5 DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC) AS seq_no_2,
        LAG(balance_bucket, 2) OVER (PARTITION BY status ORDER BY next_balance_bucket ASC, id ASC) AS prev_balance_bucket
    FROM metrics
)
SELECT
    id,
    balance,
    status,
    balance_bucket,
    next_balance_avg_5,
    cumulative_next_balance,
    next_balance_avg_5_2,
    status_sum_next_balance_bucket,
    seq_no,
    CASE WHEN status RLIKE '^(CLOSED|OPEN)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS status_zone
FROM metrics_accounts
ORDER BY seq_no DESC;
