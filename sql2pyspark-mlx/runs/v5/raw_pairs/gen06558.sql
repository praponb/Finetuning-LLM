WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE amount IS NOT NULL
      AND txn_time > TO_DATE('2023-06-16')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        is_flagged,
        days_since_txn_time,
        resolved_merchant
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY days_since_txn_time DESC NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        d.status,
        d.balance,
        d.account_type
    FROM deduped e
    JOIN accounts d
        ON e.account_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        status,
        balance,
        account_type,
        LAG(amount) OVER (PARTITION BY status ORDER BY amount, id) AS prev_amount,
        LAG(days_since_txn_time) OVER (PARTITION BY status ORDER BY amount, id) AS prev_days_since_txn_time,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS event_seq
    FROM joined
),
kept_keys AS (
    -- Set intersection on keys
    SELECT account_id
    FROM metrics
    INTERSECT ALL
    SELECT account_id
    FROM transactions
    WHERE txn_type IN ('REFUND', 'CREDIT', 'DEBIT')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.account_id = k.account_id
),
filtered_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END AS is_open,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM verified
    WHERE event_seq BETWEEN 3 AND 4
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.prev_days_since_txn_time,
    s.event_seq,
    s.is_open,
    s.status_filled,
    (SELECT MAX(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS max_transaction_amount
FROM filtered_transactions s
ORDER BY id NULLS FIRST
