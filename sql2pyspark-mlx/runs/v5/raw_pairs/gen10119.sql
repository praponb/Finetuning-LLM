WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        LOWER(txn_type) AS txn_type_lower,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM transactions
    WHERE txn_time > TO_TIMESTAMP('2023-10-24 14:00:00')
      AND merchant IS NOT NULL
),
joined AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_time,
        a.merchant,
        a.is_flagged,
        a.txn_type_lower,
        b.balance,
        b.status,
        b.account_type
    FROM filtered a
    JOIN accounts b
        ON a.account_id = b.id
    WHERE b.balance > 35000
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(merchant) AS merchant_lower
    FROM joined
    WHERE txn_time >= TO_DATE('2023-10-15')
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_time,
        s.txn_type_lower,
        s.account_type,
        s.merchant_lower,
        a.account_type AS account_account_type
    FROM valid s
    JOIN accounts a
        ON s.account_id = a.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_account_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_account_type ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_time,
        txn_type_lower,
        merchant_lower,
        account_account_type,
        SUM(is_new_burst) OVER (PARTITION BY account_account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    id,
    merchant_lower,
    account_account_type,
    CASE WHEN account_account_type RLIKE '^(CREDIT|LOAN)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS account_account_type_zone
FROM islands
