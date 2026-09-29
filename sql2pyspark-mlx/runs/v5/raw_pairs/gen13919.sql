WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(account_type) AS clean_account_type
    FROM accounts
    WHERE status IS NOT NULL
      AND customer_id = 32
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, c.city
    FROM cleaned s
    JOIN customers c
        ON s.customer_id = c.id
    WHERE c.segment IN ('SMB', 'Consumer', 'Corporate')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        opened_date,
        status,
        clean_account_type,
        city,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        COUNT(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_balance,
        MAX(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance
    FROM enriched
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_txn_type
    FROM scored s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_avg_balance >= 63000 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_balance_tier,
        account_type,
        ROUND(SUM(running_count_balance) / NULLIF(COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END), 0), 2) AS running_count_balance_per_credit,
        APPROX_PERCENTILE(running_avg_balance, ARRAY(0.25, 0.5, 0.75)) AS running_avg_balance_quartiles,
        GROUPING_ID() AS grouping_level
    FROM with_last
    GROUP BY GROUPING SETS ((CASE WHEN running_avg_balance >= 63000 THEN 'LARGE' ELSE 'SMALL' END, account_type), (CASE WHEN running_avg_balance >= 63000 THEN 'LARGE' ELSE 'SMALL' END))
    HAVING running_count_balance_per_credit > 0
)
SELECT
    running_count_balance_per_credit,
    running_avg_balance_quartiles,
    grouping_level
FROM daily
ORDER BY grouping_level, running_count_balance_per_credit;
