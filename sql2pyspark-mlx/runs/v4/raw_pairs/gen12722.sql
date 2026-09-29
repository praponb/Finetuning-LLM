WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(merchant, ' ', txn_type) AS merchant_txn_type
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-11-02 19:00:00'
      AND merchant IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_txn_type,
        ROW_NUMBER() OVER w_hist AS seq_no,
        LEAD(amount, 2) OVER (PARTITION BY account_id ORDER BY amount, id) AS next_amount,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        NTILE(10) OVER (PARTITION BY merchant ORDER BY amount, id) AS amount_bucket
    FROM base
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY txn_time DESC, id)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        b.balance,
        b.status,
        b.opened_date
    FROM scored e
    INNER JOIN accounts b
        ON e.account_id = b.id
),
joined AS (
    -- Attach accounts attributes
    SELECT s.*, a.opened_date AS account_opened_date
    FROM with_dims s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY RANK() OVER (PARTITION BY merchant ORDER BY account_opened_date) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        a.status AS account_status,
        a.balance AS account_balance
    FROM islands e
    LEFT JOIN accounts a
        ON e.account_id = a.id
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN amount_avg_2_prior >= 3822 THEN 'LARGE' ELSE 'SMALL' END AS amount_avg_2_prior_tier,
        DATE_TRUNC('YEAR', account_opened_date) AS year_start,
        CASE WHEN burst_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
        APPROX_PERCENTILE(amount_bucket, 0.95) AS amount_bucket_p95,
        AVG(burst_id) FILTER (WHERE merchant = '7-Eleven') AS 7_eleven_avg_burst_id
    FROM with_dims_transactions
    GROUP BY CUBE (CASE WHEN amount_avg_2_prior >= 3822 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', account_opened_date), CASE WHEN burst_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT year_start
FROM daily;
