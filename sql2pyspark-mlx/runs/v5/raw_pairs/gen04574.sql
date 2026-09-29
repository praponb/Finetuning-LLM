WITH valid AS (
    SELECT
        *,
        FLOOR(balance) AS balance_whole,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-04-07'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance_whole DESC, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        id,
        customer_id,
        account_type,
        status,
        balance_whole,
        resolved_status,
        LAG(balance_whole) OVER w_running AS prev_balance_whole,
        LEAD(balance) OVER w_running AS next_balance,
        ROW_NUMBER() OVER w_running AS seq_no,
        RANK() OVER (PARTITION BY customer_id, status ORDER BY balance_whole) AS balance_whole_rank
    FROM ranked
    WINDOW w_running AS (PARTITION BY account_type ORDER BY balance_whole, id)
),
with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.balance_whole,
        s.prev_balance_whole,
        s.seq_no,
        x.amount,
        x.txn_time
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched_accounts AS (
    SELECT a.*, b.email
    FROM with_latest a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
with_latest_accounts AS (
    SELECT
        s.id,
        s.prev_balance_whole,
        s.seq_no,
        s.amount,
        x.transaction_txn_type
    FROM enriched_accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    prev_balance_whole,
    seq_no,
    amount,
    transaction_txn_type,
    CASE WHEN transaction_txn_type RLIKE '^(FEE|CREDIT)$' THEN 'OTHER_ZONE' WHEN transaction_txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS transaction_txn_type_zone
FROM with_latest_accounts
