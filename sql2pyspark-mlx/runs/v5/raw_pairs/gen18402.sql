WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(txn_time) AS txn_time_date,
        CONCAT(txn_type, ', ', merchant) AS txn_type_merchant,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-06-10 07:00:00'
      AND merchant IS NOT DISTINCT FROM '7-Eleven'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        is_flagged,
        txn_type_merchant,
        NTILE(10) OVER (PARTITION BY txn_type, account_id ORDER BY amount, id) AS amount_bucket,
        ROW_NUMBER() OVER (ORDER BY txn_time_date DESC, id) AS event_seq
    FROM valid
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount_bucket, 1) OVER (ORDER BY amount, id) AS next_amount_bucket,
        NTILE(3) OVER (ORDER BY amount, id) AS amount_bucket_2
    FROM scored
),
scored_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount_bucket,
        next_amount_bucket,
        LAG(amount_bucket_2) OVER (ORDER BY event_seq, id) AS prev_amount_bucket_2,
        LEAD(amount_bucket_2) OVER (ORDER BY event_seq, id) AS next_amount_bucket_2,
        LEAD(event_seq) OVER (ORDER BY event_seq, id) AS next_event_seq,
        LAG(event_seq) OVER (ORDER BY event_seq, id) AS prev_event_seq,
        ROW_NUMBER() OVER (ORDER BY event_seq, id) AS event_seq_2,
        CUME_DIST() OVER (ORDER BY amount_bucket_2 NULLS FIRST) AS amount_bucket_2_cume_dist
    FROM with_windows
    WHERE amount_bucket_2 = 3
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS next_event_seq_tier,
    CASE WHEN amount_bucket_2_cume_dist >= 0.68 THEN 'LARGE' ELSE 'SMALL' END AS amount_bucket_2_cume_dist_tier,
    CASE WHEN next_amount_bucket_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_amount_bucket_2_tier,
    PERCENTILE_APPROX(next_event_seq, 0.5) AS next_event_seq_p50,
    MIN(amount_bucket_2_cume_dist) AS min_amount_bucket_2_cume_dist,
    PERCENTILE_APPROX(amount_bucket_2_cume_dist, 0.95) AS amount_bucket_2_cume_dist_p95
FROM scored_transactions
GROUP BY ROLLUP (CASE WHEN next_event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN amount_bucket_2_cume_dist >= 0.68 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_amount_bucket_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING min_amount_bucket_2_cume_dist >= 0;
