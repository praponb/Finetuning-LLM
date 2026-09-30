WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END is_grab,
        COALESCE(merchant, 'N/A') merchant_clean
    FROM transactions
    WHERE txn_time >= TO_DATE('2023-11-27')
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, is_grab + amount is_grab_plus_amount
    FROM valid
    WHERE txn_time > TO_DATE('2023-12-16')
      AND merchant IS NOT NULL
      AND is_flagged = TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_grab,
        merchant_clean,
        is_grab_plus_amount,
        MIN(is_grab) OVER (PARTITION BY account_id) account_id_min_is_grab,
        SUM(is_grab_plus_amount) OVER (PARTITION BY account_id, merchant ORDER BY is_grab_plus_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_grab_plus_amount
    FROM base
),
joined AS (
    -- Attach accounts attributes
    SELECT src.*, d.opened_date
    FROM with_windows src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_grab,
        merchant_clean,
        is_grab_plus_amount,
        account_id_min_is_grab,
        running_is_grab_plus_amount,
        opened_date,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) amount_share,
        SUM(account_id_min_is_grab) OVER (ORDER BY is_grab, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_account_id_min_is_grab,
        NTILE(5) OVER (PARTITION BY account_id, txn_type ORDER BY is_grab_plus_amount DESC, id) is_grab_plus_amount_bucket,
        NTILE(5) OVER w_grp is_grab_bucket,
        FIRST_VALUE(is_grab, TRUE) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_is_grab
    FROM joined
    WINDOW w_grp AS (ORDER BY is_grab DESC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
        CASE WHEN is_grab_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_grab_bucket_tier,
        account_id,
        MIN(first_known_is_grab) AS min_first_known_is_grab,
        MAX(txn_time) AS ended_at,
        SUM(CASE WHEN txn_type = 'REFUND' THEN cumulative_account_id_min_is_grab ELSE 0 END) AS refund_cumulative_account_id_min_is_grab,
        PERCENTILE_APPROX(amount_share, ARRAY(0.25, 0.5, 0.75)) AS amount_share_quartiles,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', txn_time) ORDER BY MIN(first_known_is_grab) DESC) AS min_first_known_is_grab_percent_rank
    FROM metrics
    GROUP BY DATE_TRUNC('QUARTER', txn_time), CASE WHEN is_grab_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, account_id
)
SELECT
    quarter_start,
    is_grab_bucket_tier,
    account_id,
    ended_at,
    refund_cumulative_account_id_min_is_grab,
    min_first_known_is_grab_percent_rank
FROM grouped
ORDER BY ended_at DESC NULLS LAST
