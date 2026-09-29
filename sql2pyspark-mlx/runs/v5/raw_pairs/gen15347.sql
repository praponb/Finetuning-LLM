WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(amount AS INT) AS amount_int
    FROM transactions
    WHERE txn_time > DATE '2023-10-06'
      AND merchant IS DISTINCT FROM 'Grab'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.merchant,
        e.is_flagged,
        e.session_id,
        b.status,
        b.balance
    FROM sessionized e
    INNER JOIN accounts b
        ON e.account_id = b.id
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.session_id,
        src.status,
        src.balance,
        d.balance AS account_balance,
        d.status AS account_status
    FROM enriched src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_id,
        status,
        balance,
        account_status
    FROM enriched_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY balance, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance >= 76000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    AVG(balance) AS avg_balance,
    SIZE(COLLECT_SET(account_status)) AS num_account_status,
    SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance,
    SUM(CASE WHEN account_status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance_2,
    SUM(CASE WHEN account_status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance_3
FROM ranked
GROUP BY CASE WHEN balance >= 76000 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_account_status DESC
