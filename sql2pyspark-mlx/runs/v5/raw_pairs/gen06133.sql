WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        UPPER(merchant) AS merchant_upper,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-08-10 03:00:00'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS FIRST, id) = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM deduped
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_new_session,
        SUM(is_new_session) OVER (ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_session,
        FIRST_VALUE(amount) IGNORE NULLS OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM sessions_transactions
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN first_known_amount >= 3500 THEN 'LARGE' ELSE 'SMALL' END AS first_known_amount_tier,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SIZE(COLLECT_SET(merchant)) AS num_merchants_2
FROM enriched
GROUP BY CUBE (CASE WHEN first_known_amount >= 3500 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY duration_min NULLS LAST
