WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        LOWER(merchant) AS merchant_lower,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-10-20 17:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT txn_type, merchant
    FROM sessions_transactions
    EXCEPT DISTINCT
    SELECT txn_type, merchant
    FROM transactions
    WHERE txn_type NOT IN ('REFUND', 'FEE')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_transactions s
    JOIN kept_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        merchant_filled,
        ROUND(amount * 1.15, 2) AS adjusted_amount
    FROM verified
    WHERE txn_time >= TO_TIMESTAMP('2023-05-22 01:00:00')
      AND merchant IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END AS is_fee
    FROM valid
    WHERE txn_type IN ('FEE', 'REFUND')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        txn_type,
        amount,
        merchant_filled,
        is_fee,
        COALESCE(amount, 0) AS amount_filled,
        amount * is_fee AS amount_times_is_fee
    FROM filtered
    WHERE adjusted_amount <= 76
)
SELECT
    amount,
    merchant_filled,
    is_fee,
    amount_filled
FROM base
ORDER BY amount_filled, amount
