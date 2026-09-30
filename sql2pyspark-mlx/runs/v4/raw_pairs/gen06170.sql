WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        ROUND(amount, 2) AS amount_rounded,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY account_id ORDER BY amount_rounded ASC) = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(amount * 0.9, 2) AS amount_scaled
    FROM ranked
    WHERE merchant IS NOT NULL
      AND amount_rounded <= 31
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY amount_rounded DESC, id ASC) AS seq_no,
        SUM(amount) OVER w_part AS running_amount,
        LAG(amount_rounded, 1, 0) OVER (PARTITION BY merchant ORDER BY amount ASC, id ASC) AS prev_amount_rounded,
        SUM(amount) OVER w_part_2 AS running_amount_2
    FROM cleaned
    WINDOW w_part AS (PARTITION BY account_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY amount_scaled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        amount_rounded,
        merchant_clean,
        amount_scaled,
        seq_no,
        running_amount,
        prev_amount_rounded,
        running_amount_2,
        ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY running_amount ASC, id ASC) AS seq_no_2,
        NTILE(3) OVER (PARTITION BY merchant, txn_type ORDER BY running_amount_2 DESC, id ASC) AS running_amount_2_bucket,
        LAST_VALUE(amount_scaled, TRUE) OVER (PARTITION BY txn_type ORDER BY amount_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_scaled,
        MIN(running_amount) OVER (PARTITION BY account_id) AS account_id_min_running_amount
    FROM scored
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN running_amount >= 21638 THEN 'LARGE' ELSE 'SMALL' END AS running_amount_tier,
        CASE WHEN running_amount_2_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS running_amount_2_bucket_tier,
        COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count,
        ROUND(AVG(amount), 2) AS avg_amount,
        COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count,
        COUNT(DISTINCT merchant) AS distinct_merchants,
        APPROX_PERCENTILE(running_amount_2_bucket, ARRAY(0.25, 0.5, 0.75)) AS running_amount_2_bucket_quartiles
    FROM with_windows
    GROUP BY CUBE (CASE WHEN running_amount >= 21638 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_amount_2_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    running_amount_tier,
    running_amount_2_bucket_tier,
    grab_count,
    starbucks_count,
    distinct_merchants,
    running_amount_2_bucket_quartiles
FROM stats
ORDER BY running_amount_2_bucket_tier DESC
