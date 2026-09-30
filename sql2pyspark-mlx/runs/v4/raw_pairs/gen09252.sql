WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        UPPER(merchant) AS merchant_upper
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-05-21 21:00:00'
      AND account_id IN (14, 17, 38)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        p2.account_type,
        p2.balance
    FROM ranked a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        p.opened_date,
        p.status,
        p.balance AS account_balance
    FROM enriched s
    LEFT JOIN accounts p
        ON s.account_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    ROUND(AVG(balance), 2) AS avg_balance,
    MAX(txn_time) AS last_seen,
    COUNT_IF(txn_type = 'DEBIT') AS debit_events,
    SUM(account_balance) AS total_account_balance,
    SUM(account_balance) FILTER (WHERE status = 'FROZEN') AS frozen_total_account_balance,
    MAX(account_balance) FILTER (WHERE account_type = 'CREDIT') AS credit_max_account_balance
FROM joined
GROUP BY DATE_TRUNC('MONTH', txn_time)
ORDER BY avg_balance NULLS FIRST;
