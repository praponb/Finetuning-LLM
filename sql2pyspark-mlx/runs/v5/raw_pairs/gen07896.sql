WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        start_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'Group A' ELSE 'Other' END AS status_group,
        SUBSTRING(plan, 1, 2) AS plan_prefix
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-07-26')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, start_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY start_date DESC) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        ROW_NUMBER() OVER (ORDER BY start_date, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY start_date, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY start_date, id) AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id) AS row_seq
    FROM deduped
    WHERE start_date <= '2023-11-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        start_date,
        seq_no,
        seq_no_2,
        PERCENT_RANK() OVER (ORDER BY event_seq NULLS FIRST) AS event_seq_percent_rank,
        AVG(event_seq) OVER (ORDER BY seq_no_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
        ROW_NUMBER() OVER (ORDER BY row_seq DESC, id) AS seq_no_3
    FROM enriched
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        seq_no,
        seq_no_2,
        event_seq_avg_3,
        seq_no_3,
        ROW_NUMBER() OVER (ORDER BY event_seq_percent_rank DESC, id) AS row_seq,
        NTILE(5) OVER (ORDER BY event_seq_avg_3, id) AS event_seq_avg_3_bucket
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    CASE WHEN seq_no_3 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_3_tier,
    CASE WHEN event_seq_avg_3_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_avg_3_bucket_tier,
    PERCENTILE_APPROX(row_seq, 0.9) AS row_seq_p90,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles_2,
    APPROX_PERCENTILE(row_seq, 0.9) AS row_seq_p90_2,
    PERCENTILE_APPROX(event_seq_avg_3_bucket, 0.5) AS event_seq_avg_3_bucket_p50,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles_3,
    DENSE_RANK() OVER (PARTITION BY seq_no ORDER BY APPROX_PERCENTILE(row_seq, 0.9) DESC) AS row_seq_p90_2_dense_rank
FROM enriched_subscriptions
GROUP BY seq_no, CASE WHEN seq_no_3 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN event_seq_avg_3_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING PERCENTILE_APPROX(event_seq_avg_3_bucket, 0.5) > 0
ORDER BY event_seq_avg_3_bucket_p50 DESC NULLS LAST
