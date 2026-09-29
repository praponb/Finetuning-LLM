WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM subscriptions
    WHERE monthly_fee > 70
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        NTILE(10) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS monthly_fee_bucket_2,
        LEAD(monthly_fee, 1, 0) OVER w_grp AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY end_date DESC, id) AS event_seq
    FROM deduped
    WHERE monthly_fee BETWEEN 90 AND 230
    WINDOW w_grp AS (PARTITION BY plan ORDER BY start_date, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_monthly_fee DESC, id) AS event_seq_2,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY first_known_monthly_fee, id) AS first_known_monthly_fee_bucket
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_bucket_2 DESC NULLS FIRST) AS monthly_fee_bucket_2_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_bucket_2 DESC NULLS LAST) AS monthly_fee_bucket_2_percent_rank
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(next_monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY monthly_fee_bucket_2_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY next_monthly_fee, id) AS row_seq,
        SUM(event_seq_2) OVER (PARTITION BY customer_id) AS customer_id_sum_event_seq_2,
        PERCENT_RANK() OVER (ORDER BY monthly_fee_bucket_2_rank NULLS FIRST) AS monthly_fee_bucket_2_rank_percent_rank,
        LEAD(first_known_monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY first_known_monthly_fee_bucket, id) AS next_first_known_monthly_fee
    FROM with_windows
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.monthly_fee_bucket,
        e.monthly_fee_bucket_2,
        e.event_seq_2,
        e.first_known_monthly_fee_bucket,
        e.monthly_fee_bucket_2_percent_rank,
        e.first_known_next_monthly_fee,
        e.row_seq,
        e.customer_id_sum_event_seq_2,
        e.monthly_fee_bucket_2_rank_percent_rank,
        b.age,
        b.email,
        b.name
    FROM scored e
    JOIN customers b
        ON e.customer_id = b.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
        customer_id,
        APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(first_known_monthly_fee_bucket, ARRAY(0.25, 0.5, 0.75)) AS first_known_monthly_fee_bucket_quartiles,
        AVG(monthly_fee_bucket_2) AS avg_monthly_fee_bucket_2,
        SUM(monthly_fee_bucket) AS total_monthly_fee_bucket,
        PERCENTILE_APPROX(first_known_next_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS first_known_next_monthly_fee_quartiles,
        GROUPING_ID() AS gid
    FROM enriched_subscriptions
    GROUP BY CUBE (CASE WHEN row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, customer_id)
)
SELECT
    row_seq_tier,
    age_quartiles,
    event_count,
    first_known_monthly_fee_bucket_quartiles,
    total_monthly_fee_bucket,
    gid
FROM totals
ORDER BY row_seq_tier DESC;
