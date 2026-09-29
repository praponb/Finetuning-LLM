WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        LENGTH(status) AS status_length
    FROM subscriptions
    WHERE end_date >= DATE '2023-11-28'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        status_length,
        RANK() OVER (PARTITION BY customer_id, status ORDER BY status_length NULLS LAST) AS status_length_rank,
        LEAD(status_length, 2) OVER (PARTITION BY status ORDER BY start_date, id) AS next_status_length,
        RANK() OVER w_grp AS status_length_rank_2,
        PERCENT_RANK() OVER w_grp AS status_length_percent_rank,
        NTILE(5) OVER w_part AS status_length_bucket
    FROM base
    WINDOW w_grp AS (ORDER BY status_length DESC), w_part AS (ORDER BY status_length DESC, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY status_length_rank, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY status_length, id) AS seq_no,
        FIRST_VALUE(status_length_rank) IGNORE NULLS OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_length_rank,
        AVG(status_length) OVER (PARTITION BY plan, customer_id ORDER BY status_length_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_length_avg_3,
        SUM(status_length_rank) OVER (PARTITION BY status ORDER BY status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_length_rank
    FROM with_windows
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY next_status_length DESC NULLS FIRST, id) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
ranked_subscriptions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cumulative_status_length_rank DESC, id) = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.name,
        b.signup_date
    FROM ranked_subscriptions src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
ranked_subscriptions_2 AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY next_status_length DESC NULLS FIRST, id) AS dedup_rank
        FROM with_dims
    ) x
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_length_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END AS status_length_bucket_tier,
    CASE WHEN status_length_percent_rank >= 0.7 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_length_percent_rank_tier,
    SUM(status_length_bucket) FILTER (WHERE status = 'EXPIRED') AS expired_total_status_length_bucket,
    PERCENTILE_APPROX(first_known_status_length_rank, 0.9) AS first_known_status_length_rank_p90,
    COUNT(CASE WHEN status = 'PAUSED' THEN 1 END) AS paused_count,
    MIN(status_length_percent_rank) AS min_status_length_percent_rank,
    COUNT_IF(status = 'CANCELLED') AS cancelled_events
FROM ranked_subscriptions_2
GROUP BY CUBE (CASE WHEN status_length_bucket >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_length_percent_rank >= 0.7 THEN 'PREMIUM' ELSE 'STANDARD' END)
