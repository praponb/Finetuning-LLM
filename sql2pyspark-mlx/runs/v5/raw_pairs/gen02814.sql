WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END AS is_7_eleven
    FROM transactions
    WHERE merchant IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT merchant, account_id
    FROM filtered
    INTERSECT
    SELECT merchant, account_id
    FROM transactions
    WHERE account_id IN (20, 25, 34, 35)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.merchant = k.merchant AND s.account_id = k.account_id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        account_id,
        txn_time,
        COUNT(is_7_eleven) OVER (PARTITION BY account_id) AS account_id_count_is_7_eleven,
        PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank,
        MAX(is_7_eleven) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_7_eleven_max_90d,
        DENSE_RANK() OVER w_running AS is_7_eleven_dense_rank,
        SUM(is_7_eleven) OVER w_part AS account_id_sum_is_7_eleven
    FROM verified
    WINDOW w_running AS (PARTITION BY merchant ORDER BY is_7_eleven DESC NULLS LAST), w_part AS (PARTITION BY account_id, txn_type)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT a.*, d.status
    FROM enriched a
    INNER JOIN accounts d
        ON a.account_id = d.id
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        e.account_id,
        e.txn_time,
        e.is_7_eleven_max_90d,
        e.is_7_eleven_dense_rank,
        e.account_id_sum_is_7_eleven,
        a.balance
    FROM with_dims e
    INNER JOIN accounts a
        ON e.account_id = a.id
    WHERE MONTH(a.opened_date) IN (3, 6, 11)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    account_id,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    MIN(txn_time) AS first_seen,
    MAX(txn_time) AS last_txn_time
FROM with_dims_transactions
GROUP BY 1, 2
ORDER BY account_id NULLS LAST;
