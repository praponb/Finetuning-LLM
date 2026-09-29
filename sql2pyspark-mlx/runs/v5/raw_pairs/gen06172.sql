WITH filtered AS (
    SELECT
        *,
        ABS(balance) AS abs_balance,
        CONCAT(account_type, ', ', status) AS account_type_status,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-08-10'
      AND YEAR(opened_date) >= 2024
),
with_latest AS (
    SELECT
        s.*,
        top1.transaction_txn_time,
        top1.transaction_txn_type
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered_accounts AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        account_type_status,
        transaction_txn_time,
        INITCAP(TRIM(transaction_txn_type)) AS transaction_txn_type_title,
        COALESCE(transaction_txn_type, 'DIRECT') AS transaction_txn_type_filled
    FROM with_latest
    WHERE opened_date > DATE '2023-09-20'
      AND status IS NOT NULL
),
with_dims AS (
    SELECT s.*, c.segment
    FROM filtered_accounts s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
matched AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.transaction_txn_time >= sub.start_date
       AND src.transaction_txn_time < sub.end_date + INTERVAL 7 DAYS
),
kept_keys AS (
    SELECT customer_id, status
    FROM matched
    INTERSECT ALL
    SELECT customer_id, status
    FROM accounts
    WHERE account_type IN ('SAVINGS', 'CREDIT', 'CHECKING')
),
reconciled AS (
    SELECT s.*
    FROM matched s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.status,
    s.transaction_txn_time,
    s.transaction_txn_type_filled,
    s.segment,
    s.subscription_id,
    s.plan,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM eligible s;
