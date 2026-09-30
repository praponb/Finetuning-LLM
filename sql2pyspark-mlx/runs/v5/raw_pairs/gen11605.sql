WITH enriched AS (
    SELECT
        s.*,
        d.account_type,
        d.status
    FROM transactions s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
enriched_transactions AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS LAST) AS amount_rank,
        MAX(amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM enriched
    WHERE account_id IN (6, 14, 17, 28)
),
enriched_transactions_2 AS (
    SELECT e.*, p.opened_date
    FROM enriched_transactions e
    INNER JOIN accounts p
        ON e.account_id = p.id
),
kept_keys AS (
    SELECT id, merchant
    FROM enriched_transactions_2
    INTERSECT ALL
    SELECT id, merchant
    FROM transactions
    WHERE merchant NOT IN ('Lazada', 'Amazon', 'Starbucks')
),
kept_rows AS (
    SELECT s.*
    FROM enriched_transactions_2 s
    INNER JOIN kept_keys k ON s.id = k.id AND s.merchant = k.merchant
),
joined AS (
    SELECT s.*, d.account_type AS account_account_type
    FROM kept_rows s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
joined_transactions AS (
    SELECT
        a.*,
        d.status AS account_status,
        d.account_type AS account_account_type_2
    FROM joined a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
enriched_transactions_3 AS (
    SELECT
        a.account_id,
        a.txn_type,
        a.merchant,
        a.is_flagged,
        a.account_type,
        a.amount_max_30d,
        a.cumulative_amount,
        a.opened_date,
        a.account_account_type,
        a.account_account_type_2,
        b.account_type AS account_account_type_3,
        b.status AS account_status_2,
        b.opened_date AS account_opened_date
    FROM joined_transactions a
    LEFT JOIN accounts b
        ON a.account_id = b.id
)
SELECT
    account_account_type,
    DATE_TRUNC('MONTH', account_opened_date) AS activity_month,
    SIZE(COLLECT_SET(account_account_type_3)) AS num_account_account_type_3s,
    COUNT_IF(txn_type = 'CREDIT') AS credit_events,
    SUM(CASE WHEN account_status_2 = 'CLOSED' THEN amount_max_30d ELSE 0 END) AS closed_amount_max_30d
FROM enriched_transactions_3
GROUP BY account_account_type, activity_month
