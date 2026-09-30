WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        TRIM(status) AS clean_status,
        ABS(balance) AS abs_balance
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-05-22')
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.status,
        s.abs_balance,
        top1.txn_time
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_last
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(status) IGNORE NULLS OVER w_ordered AS first_known_status,
        MAX(is_new_burst) OVER w_hist AS is_new_burst_max_7d,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM sessionized
    WHERE YEAR(opened_date) = 2025
    WINDOW w_ordered AS (PARTITION BY status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY status ORDER BY is_new_burst, id)
)
SELECT
    id,
    opened_date,
    status,
    abs_balance,
    txn_time,
    is_new_burst,
    first_known_status,
    is_new_burst_max_7d,
    seq_no
FROM metrics
ORDER BY status DESC
