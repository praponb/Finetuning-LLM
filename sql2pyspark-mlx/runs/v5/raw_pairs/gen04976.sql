WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM transactions
    QUALIFY RANK() OVER (PARTITION BY txn_type ORDER BY txn_time NULLS FIRST) = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.is_flagged,
        s.burst_id,
        d.account_type
    FROM sessions_transactions s
    INNER JOIN accounts d
        ON s.account_id = d.id
),
ranked_transactions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        account_id,
        is_flagged,
        account_type
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY burst_id DESC) AS rank_in_group
        FROM with_dims
    ) t
    WHERE rank_in_group = 1
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.account_type,
        p.balance,
        p.account_type AS account_account_type,
        p.opened_date
    FROM ranked_transactions s
    LEFT JOIN accounts p
        ON s.account_id = p.id
)
SELECT
    id,
    account_id,
    balance,
    account_account_type,
    opened_date
FROM joined;
