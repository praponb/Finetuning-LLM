WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        CASE WHEN balance >= 85000 THEN 'High' WHEN balance >= 26000 THEN 'Medium' ELSE 'Low' END AS balance_band,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-02-27'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance DESC, id) AS rn
    FROM base
    QUALIFY rn = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        status,
        rn,
        CONCAT(status, ' - ', account_type) AS status_account_type
    FROM latest
    WHERE balance > 16000
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        NTILE(10) OVER (PARTITION BY status ORDER BY balance, id) AS balance_bucket,
        SUM(balance) OVER (PARTITION BY account_type, status ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance DESC, id) AS event_seq
    FROM valid
    WHERE balance > 60000
),
latest_accounts AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY running_balance DESC NULLS LAST, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        MIN(balance_bucket) OVER w_running AS last4_min_balance_bucket,
        MAX(balance_bucket) OVER w_running AS last4_max_balance_bucket,
        AVG(balance_bucket) OVER w_running AS last4_avg_balance_bucket,
        COUNT(balance_bucket) OVER w_running AS last4_count_balance_bucket
    FROM latest_accounts
    WINDOW w_running AS (PARTITION BY account_type ORDER BY rn, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Window metrics over several different partitions
SELECT
    id,
    last4_min_balance_bucket,
    last4_max_balance_bucket,
    last4_avg_balance_bucket,
    LAG(last4_avg_balance_bucket) OVER (PARTITION BY account_type ORDER BY last4_min_balance_bucket, id) AS prev_last4_avg_balance_bucket,
    LEAD(last4_avg_balance_bucket) OVER (PARTITION BY account_type ORDER BY last4_min_balance_bucket, id) AS next_last4_avg_balance_bucket,
    LEAD(last4_max_balance_bucket) OVER (PARTITION BY account_type ORDER BY last4_min_balance_bucket, id) AS next_last4_max_balance_bucket,
    ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY last4_min_balance_bucket, id) AS row_seq,
    FIRST_VALUE(last4_count_balance_bucket, TRUE) OVER (ORDER BY last4_min_balance_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_count_balance_bucket
FROM with_windows;
