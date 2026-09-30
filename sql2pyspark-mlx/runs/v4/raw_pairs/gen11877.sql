WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(merchant, ' - ', txn_type) AS merchant_txn_type,
        CONCAT(txn_type, ' - ', merchant) AS txn_type_merchant,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE txn_type != 'DEBIT'
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.status,
        a.opened_date,
        a.account_type
    FROM filtered s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    APPROX_PERCENTILE(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles,
    COUNT_IF(txn_type = 'CREDIT') AS credit_events,
    COUNT_IF(account_type = 'LOAN') AS loan_events
FROM islands
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', opened_date), DATE_TRUNC('WEEK', txn_time))
ORDER BY loan_events;
