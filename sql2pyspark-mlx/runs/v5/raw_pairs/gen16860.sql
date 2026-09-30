WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < '2023-07-01'
        )
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.status,
        latest.transaction_merchant
    FROM eligible s
    JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(transaction_merchant, ' - ', status) AS transaction_merchant_status
    FROM latest_match
    WHERE transaction_merchant != 'Starbucks'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(transaction_merchant, ', ', status) AS transaction_merchant_status_2
    FROM base
    WHERE status IS NULL
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_txn_type,
        x.amount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 4300 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN status = 'CLOSED' THEN amount ELSE 0 END) AS closed_amount,
    APPROX_PERCENTILE(amount, 0.95) AS amount_p95
FROM with_last
GROUP BY 1
ORDER BY closed_amount DESC;
