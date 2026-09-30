WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        CASE WHEN amount >= 3900 THEN 'High' WHEN amount >= 900 THEN 'Medium' ELSE 'Low' END AS amount_level,
        COALESCE(merchant, 'NONE') AS merchant_clean
    FROM transactions
    WHERE account_id IN (4, 21)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        a.account_type,
        a.opened_date
    FROM valid e
    INNER JOIN accounts a
        ON e.account_id = a.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        amount_level,
        opened_date,
        LAST_VALUE(account_type, TRUE) OVER (PARTITION BY txn_type, account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_account_type,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time, id) AS seq_no,
        FIRST_VALUE(account_type, TRUE) OVER w_recent AS first_known_account_type,
        FIRST_VALUE(account_type, TRUE) OVER (PARTITION BY txn_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type_2
    FROM with_dims
    WHERE MONTH(opened_date) IN (1, 8, 10)
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        amount_level,
        last_known_account_type,
        row_seq,
        seq_no,
        first_known_account_type,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        MONTH(txn_time) AS txn_time_month
    FROM metrics
    WHERE txn_time >= TO_TIMESTAMP('2023-09-16 08:00:00')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        amount_level,
        row_seq,
        seq_no,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM with_gaps
)
-- Join a dimension and aggregate
SELECT
    a.status,
    COUNT(*) AS num_rows,
    ROUND(AVG(src.island_id), 2) AS avg_island_id
FROM islands src
INNER JOIN accounts a ON src.account_id = a.id
GROUP BY a.status;
