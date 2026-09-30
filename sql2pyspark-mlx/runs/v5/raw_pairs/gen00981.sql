WITH with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.merchant,
        top1.transaction_txn_type
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.merchant, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.txn_time
    FROM with_last s
    JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'CREDIT'
        )
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY transaction_txn_type ORDER BY txn_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM kept
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        balance,
        opened_date,
        status,
        transaction_txn_type,
        txn_time,
        SUM(session_start_flag) OVER (PARTITION BY transaction_txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM islands
    EXCEPT ALL
    SELECT id
    FROM accounts
    WHERE status = 'CLOSED'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        balance,
        status,
        session_no,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM verified
    WHERE status IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(days_since_opened_date, balance) AS least_days_since_opened_date_balance,
        ROUND(days_since_opened_date, 2) AS days_since_opened_date_rounded,
        COALESCE(status, 'OTHER') AS status_clean
    FROM base
    WHERE session_no > 6
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    status,
    measure,
    reading
FROM (
    SELECT
        status,
        least_days_since_opened_date_balance,
        days_since_opened_date_rounded,
        balance
    FROM cleaned
) src
UNPIVOT (
    reading FOR measure IN (least_days_since_opened_date_balance AS LEAST_DAYS_SINCE_OPENED_DATE_BALANCE, days_since_opened_date_rounded AS DAYS_SINCE_OPENED_DATE_ROUNDED, balance AS BALANCE)
);
