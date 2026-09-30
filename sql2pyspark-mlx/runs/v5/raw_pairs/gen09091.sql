WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        LOWER(method) method_lower
    FROM payments
    WHERE order_id IS NOT NULL
      AND HOUR(paid_at) BETWEEN 10 AND 17
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY method ORDER BY amount) amount_rank,
        ROW_NUMBER() OVER w_part seq_no,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) amount_share,
        AVG(amount) OVER w_recent amount_avg_6,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) amount_share_2
    FROM cleaned
    WINDOW w_part AS (PARTITION BY method ORDER BY amount DESC, id), w_recent AS (ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
cleaned_payments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(amount_rank AS DOUBLE) amount_rank_str,
        COALESCE(method, 'UNKNOWN') resolved_method
    FROM scored
    WHERE method IS NOT NULL
),
scored_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(seq_no) OVER (PARTITION BY method ORDER BY amount_avg_6, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) seq_no_avg_2_prior,
        NTILE(4) OVER (ORDER BY seq_no, id) seq_no_bucket,
        LAST_VALUE(amount_rank_str, TRUE) OVER (PARTITION BY method ORDER BY amount_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount_rank_str,
        SUM(amount_share) OVER (PARTITION BY method) method_sum_amount_share,
        NTILE(10) OVER (PARTITION BY method ORDER BY amount_rank, id) amount_rank_bucket
    FROM cleaned_payments
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount_rank_bucket / SUM(amount_rank_bucket) OVER (PARTITION BY method), 4) amount_rank_bucket_share,
        DENSE_RANK() OVER (ORDER BY seq_no_avg_2_prior NULLS FIRST) seq_no_avg_2_prior_dense_rank,
        NTILE(5) OVER (PARTITION BY method ORDER BY method_sum_amount_share DESC, id) method_sum_amount_share_bucket,
        ROW_NUMBER() OVER w_running seq_no_2
    FROM scored_payments
    WINDOW w_running AS (PARTITION BY method ORDER BY amount_share DESC, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN seq_no_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_bucket_tier,
    CASE WHEN seq_no_avg_2_prior >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_avg_2_prior_tier,
    CASE WHEN amount_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END AS amount_rank_tier,
    COUNT_IF(method = 'cash') AS cash_events,
    SUM(seq_no) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_total_seq_no,
    SIZE(COLLECT_SET(method)) AS num_methods,
    ROUND(SUM(amount_rank_bucket_share) / NULLIF(COUNT(CASE WHEN method = 'cash' THEN 1 END), 0), 2) AS amount_rank_bucket_share_per_cash,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN seq_no_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY SIZE(COLLECT_SET(method)) DESC) AS num_methods_percent_rank
FROM enriched
GROUP BY 1, 2, 3
ORDER BY seq_no_bucket_tier NULLS LAST
