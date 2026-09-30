WITH enriched AS (
    -- Attach accounts attributes
    SELECT e.*, d.opened_date
    FROM transactions e
    LEFT JOIN accounts d
        ON e.account_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(amount, 1) OVER (PARTITION BY merchant ORDER BY opened_date, id) AS prev_amount,
        COUNT(amount) OVER (PARTITION BY merchant) AS merchant_count_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        opened_date,
        prev_amount,
        merchant_count_amount,
        amount_share,
        FIRST_VALUE(amount_share) IGNORE NULLS OVER w_ordered AS first_known_amount_share,
        NTILE(10) OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS amount_bucket,
        RANK() OVER (ORDER BY amount DESC NULLS LAST) AS amount_rank,
        NTILE(4) OVER w_recent AS amount_bucket_2
    FROM scored
    WINDOW w_ordered AS (ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY merchant, txn_type ORDER BY amount, id)
),
joined AS (
    -- Attach accounts attributes
    SELECT e.*, b.account_type
    FROM metrics e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM joined
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.opened_date AS account_opened_date,
        a.balance
    FROM islands s
    LEFT JOIN accounts a
        ON s.account_id = a.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        txn_type,
        account_type,
        session_id
    FROM enriched_transactions
) src
PIVOT (
    SUM(session_id) AS amount,
    COUNT(1) AS n
    FOR account_type IN ('CREDIT' AS credit, 'SAVINGS' AS savings)
);
