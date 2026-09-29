WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(amount AS INT) amount_int
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time < '2024-12-29'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(amount_int, TRUE) OVER (PARTITION BY merchant ORDER BY is_new_session ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount_int,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY txn_type, merchant ORDER BY amount_int ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_merchant
    FROM sessions_transactions
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    amount_int,
    is_new_session,
    session_no,
    CASE WHEN txn_type RLIKE '^(DEBIT|FEE)$' THEN 'GROUP_B' WHEN txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS txn_type_zone
FROM metrics
ORDER BY amount ASC;
