WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN account_type IN ('CREDIT', 'LOAN') THEN 'Tier 1' ELSE 'Other' END AS account_type_group,
        SUBSTRING(account_type, 1, 3) AS account_type_prefix
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'OPEN'
      AND status IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(balance) OVER (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_balance,
        COUNT(balance) OVER (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_balance,
        AVG(balance) OVER (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_balance,
        AVG(balance) OVER (PARTITION BY account_type, customer_id ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior
    FROM base
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        account_type_prefix,
        last7_count_balance,
        balance_avg_5_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance_avg_5_prior NULLS LAST, id) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.last7_count_balance,
        latest.transaction_txn_time
    FROM deduped s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.last7_count_balance,
        latest.transaction_txn_time_2
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    last7_count_balance,
    transaction_txn_time_2
FROM with_latest
ORDER BY transaction_txn_time_2 DESC
