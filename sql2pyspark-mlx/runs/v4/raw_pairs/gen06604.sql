WITH cleaned AS (
    SELECT *, UPPER(merchant) AS merchant_upper
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-12-11 09:00:00')
      AND merchant IS NOT NULL
),
valid AS (
    SELECT *, INITCAP(TRIM(merchant)) AS merchant_title
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND HOUR(txn_time) BETWEEN 11 AND 17
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
valid_transactions AS (
    SELECT
        id,
        amount,
        txn_time,
        merchant_upper,
        merchant_title,
        dedup_rank,
        CAST(txn_time AS DATE) AS txn_time_day,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM ranked
    WHERE txn_time > TO_DATE('2023-10-30')
),
summary AS (
    SELECT
        DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
        APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
        MAX(amount) AS max_amount,
        (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
        STDDEV_SAMP(amount) AS amount_stddev
    FROM valid_transactions
    GROUP BY DATE_TRUNC('QUARTER', txn_time)
    HAVING amount_stddev < 0
)
SELECT
    quarter_start,
    amount_quartiles,
    max_amount,
    amount_stddev
FROM summary;
