WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank,
        LAG(amount, 1) OVER (ORDER BY amount, id) AS prev_amount,
        LAG(amount, 1) OVER (PARTITION BY txn_type ORDER BY amount, id) AS prev_amount_2
    FROM transactions
),
joined AS (
    -- Attach accounts attributes
    SELECT a.*, p2.status
    FROM with_windows a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM joined
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY is_new_burst NULLS FIRST, id) AS rn
        FROM sessionized
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        b.account_type,
        b.balance,
        b.status AS account_status
    FROM latest e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        account_status,
        account_type,
        DATE_TRUNC('YEAR', txn_time) AS year_start,
        COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count,
        APPROX_PERCENTILE(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
        APPROX_PERCENTILE(is_new_burst, 0.9) AS is_new_burst_p90
    FROM enriched
    GROUP BY account_status, account_type, DATE_TRUNC('YEAR', txn_time)
)
SELECT
    account_status,
    account_type,
    year_start,
    savings_count,
    island_id_quartiles
FROM stats
ORDER BY year_start NULLS LAST
