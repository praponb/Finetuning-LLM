WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_type = 'FEE'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_time,
        merchant_filled,
        MIN(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_new_group_flag,
        COUNT(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_new_group_flag,
        AVG(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_new_group_flag
    FROM sessions_transactions
    WHERE burst_id != 4
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_time,
        merchant_filled,
        last3_min_new_group_flag
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY last3_avg_new_group_flag DESC NULLS FIRST, id) = 1
),
cleaned_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(last3_min_new_group_flag) AS last3_min_new_group_flag_whole
    FROM deduped
    WHERE txn_time >= TO_TIMESTAMP('2023-09-27 04:00:00')
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        id,
        last3_min_new_group_flag,
        last3_min_new_group_flag_whole,
        NTILE(4) OVER (ORDER BY last3_min_new_group_flag_whole DESC, id) AS last3_min_new_group_flag_whole_bucket,
        ROW_NUMBER() OVER (ORDER BY last3_min_new_group_flag_whole, id) AS row_seq,
        FIRST_VALUE(last3_min_new_group_flag_whole, TRUE) OVER (ORDER BY last3_min_new_group_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_min_new_group_flag_whole,
        RANK() OVER (ORDER BY last3_min_new_group_flag DESC NULLS FIRST) AS last3_min_new_group_flag_rank,
        COUNT(last3_min_new_group_flag_whole) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS last3_min_new_group_flag_whole_count_7d
    FROM cleaned_transactions
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(row_seq, 1) OVER (ORDER BY first_known_last3_min_new_group_flag_whole, id) AS next_row_seq,
        PERCENT_RANK() OVER (ORDER BY last3_min_new_group_flag_whole_bucket NULLS LAST) AS last3_min_new_group_flag_whole_bucket_percent_rank,
        ROW_NUMBER() OVER (ORDER BY last3_min_new_group_flag_whole_count_7d DESC, id) AS seq_no
    FROM metrics_transactions
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN last3_min_new_group_flag_whole_count_7d >= 16 THEN 'HIGH' ELSE 'LOW' END AS last3_min_new_group_flag_whole_count_7d_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(row_seq) AS total_row_seq,
        PERCENTILE_APPROX(last3_min_new_group_flag_whole, 0.5) AS last3_min_new_group_flag_whole_p50,
        COUNT(*) AS row_count
    FROM enriched
    GROUP BY CASE WHEN last3_min_new_group_flag_whole_count_7d >= 16 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    last3_min_new_group_flag_whole_count_7d_tier,
    total_row_seq,
    last3_min_new_group_flag_whole_p50
FROM daily
ORDER BY total_row_seq DESC
