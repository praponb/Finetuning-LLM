WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END AS is_fee,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time > TIMESTAMP '2023-06-08 06:00:00'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_time,
        merchant,
        is_flagged,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label,
        SUBSTRING(merchant, 1, 3) AS merchant_prefix,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM sessionized
    WHERE txn_time >= TO_TIMESTAMP('2023-10-07 07:00:00')
      AND amount > 3500
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(merchant)) AS merchant_title
    FROM filtered
    WHERE txn_time >= TO_TIMESTAMP('2023-08-13 14:00:00')
      AND is_flagged = TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT merchant, id
    FROM valid
    EXCEPT
    SELECT merchant, id
    FROM transactions
    WHERE txn_type = 'FEE'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.merchant = k.merchant AND s.id = k.id
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
        COUNT(DISTINCT merchant) AS distinct_merchants,
        MIN(txn_time) AS first_seen,
        COUNT_IF(merchant = 'Starbucks') AS starbucks_events
    FROM verified
    GROUP BY quarter_start
    HAVING distinct_merchants >= 1
)
SELECT
    quarter_start,
    first_seen,
    starbucks_events
FROM grouped
ORDER BY starbucks_events ASC NULLS LAST;
