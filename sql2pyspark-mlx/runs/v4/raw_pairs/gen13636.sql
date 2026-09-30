WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(balance) AS balance_whole,
        ROUND(balance, 1) AS balance_rounded,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-08-10'
      AND balance BETWEEN 70000 AND 72000
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_whole,
        balance_rounded,
        status_filled,
        AVG(balance_whole) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS balance_whole_avg_5,
        LAG(balance_rounded, 1) OVER (ORDER BY balance, id) AS prev_balance_rounded
    FROM base
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.txn_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        balance_whole / NULLIF(prev_balance_rounded, 0) AS balance_whole_per_prev_balance_rounded,
        COALESCE(txn_type, 'DIRECT') AS resolved_txn_type
    FROM with_latest
    WHERE balance_whole_avg_5 > 48
),
summary AS (
    -- Grouped metrics
    SELECT
        account_type,
        SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
        SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(balance_whole_avg_5, 0.95) AS balance_whole_avg_5_p95,
        COUNT_IF(account_type = 'SAVINGS') AS savings_events
    FROM filtered
    GROUP BY account_type
)
SELECT
    account_type,
    num_txn_types,
    account_type_list,
    num_rows,
    savings_events
FROM summary
ORDER BY account_type NULLS FIRST, num_txn_types DESC;
