WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        AVG(amount) OVER (ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3
    FROM transactions
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY amount_avg_3 NULLS LAST) AS amount_avg_3_percent_rank,
        AVG(amount_avg_3) OVER (PARTITION BY txn_type ORDER BY amount_avg_3, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_avg_3_prior,
        LAG(amount_avg_3, 1) OVER (PARTITION BY txn_type, account_id ORDER BY amount_avg_3, id) AS prev_amount_avg_3
    FROM with_windows
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY amount_avg_3 DESC, id) AS amount_avg_3_bucket,
        AVG(amount_avg_3_percent_rank) OVER (ORDER BY amount_avg_3, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_percent_rank_avg_3_prior,
        LEAD(amount_avg_3_avg_3_prior, 2) OVER (PARTITION BY account_id ORDER BY amount_avg_3_avg_3_prior, id) AS next_amount_avg_3_avg_3_prior,
        NTILE(5) OVER (PARTITION BY txn_type ORDER BY prev_amount_avg_3, id) AS prev_amount_avg_3_bucket,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY prev_amount_avg_3 DESC, id) AS event_seq
    FROM enriched
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        amount_avg_3_percent_rank,
        prev_amount_avg_3,
        amount_avg_3_bucket,
        next_amount_avg_3_avg_3_prior,
        prev_amount_avg_3_bucket,
        event_seq
    FROM with_windows_transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount_avg_3_avg_3_prior NULLS FIRST, id) = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_amount_avg_3_avg_3_prior >= 4382 THEN 'HIGH' ELSE 'LOW' END AS next_amount_avg_3_avg_3_prior_tier,
    CASE WHEN prev_amount_avg_3_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS prev_amount_avg_3_bucket_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(next_amount_avg_3_avg_3_prior, 0.95) AS next_amount_avg_3_avg_3_prior_p95,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN next_amount_avg_3_avg_3_prior >= 4382 THEN 'HIGH' ELSE 'LOW' END ORDER BY APPROX_PERCENTILE(next_amount_avg_3_avg_3_prior, 0.95) DESC) AS next_amount_avg_3_avg_3_prior_p95_percent_rank
FROM deduped
GROUP BY next_amount_avg_3_avg_3_prior_tier, prev_amount_avg_3_bucket_tier;
