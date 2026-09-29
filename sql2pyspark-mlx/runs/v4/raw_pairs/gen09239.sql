WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
matched_keys AS (
    -- Set difference on keys
    SELECT txn_type, merchant
    FROM islands
    EXCEPT
    SELECT txn_type, merchant
    FROM transactions
    WHERE account_id IN (11, 13, 17, 22)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN matched_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        new_group_flag,
        burst_id,
        INITCAP(TRIM(txn_type)) AS txn_type_title,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM verified
    WHERE txn_time >= TO_TIMESTAMP('2023-12-30 13:00:00')
)
SELECT
    id,
    txn_time,
    new_group_flag,
    burst_id,
    txn_type_title
FROM filtered
ORDER BY new_group_flag DESC;
