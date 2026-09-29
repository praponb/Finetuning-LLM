WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LAST_DAY(opened_date) AS opened_date_month_end,
        CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END AS is_closed
    FROM accounts
    WHERE opened_date >= DATE '2023-05-06'
      AND status IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.opened_date,
        s.is_closed,
        x.amount,
        x.txn_time
    FROM eligible s
    JOIN LATERAL (
            SELECT c.amount, c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.merchant,
        x.transaction_txn_type
    FROM with_last s
    JOIN LATERAL (
            SELECT c.merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.opened_date,
    s.amount,
    s.txn_time,
    s.merchant,
    s.transaction_txn_type,
    (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_amount
FROM with_latest s
ORDER BY min_order_amount DESC;
