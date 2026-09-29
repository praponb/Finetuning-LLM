WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY amount NULLS FIRST) AS row_num
        FROM transactions
    ) x
    WHERE row_num = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        b.opened_date,
        b.balance,
        b.status
    FROM sessionized s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT s.*, a.status AS account_status
    FROM joined s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            account_status,
            is_new_burst
        FROM with_dims
    ) src
    PIVOT (
        MAX(is_new_burst) AS amount
        FOR account_status IN ('FROZEN' AS frozen, 'OPEN' AS open, 'CLOSED' AS closed)
    )
)
SELECT
    status,
    open,
    closed,
    CASE WHEN status RLIKE '^(FROZEN|OPEN)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS status_zone
FROM pivoted;
