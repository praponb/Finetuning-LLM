WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        delivered_date,
        cost
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC NULLS FIRST, id ASC) AS dedup_rank
        FROM shipments
    ) ranked
    WHERE dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        delivered_date,
        LAG(cost) OVER w_part AS prev_cost,
        LEAD(cost) OVER w_part AS next_cost,
        ROW_NUMBER() OVER w_part AS event_seq,
        NTILE(4) OVER (PARTITION BY carrier ORDER BY cost DESC, id ASC) AS cost_bucket
    FROM ranked
    WHERE cost > 37
    WINDOW w_part AS (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_bucket DESC NULLS FIRST, id ASC) AS dedup_rank
    FROM metrics
    QUALIFY dedup_rank = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        delivered_date,
        next_cost,
        event_seq,
        cost_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY next_cost DESC NULLS LAST, id ASC) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(event_seq, 0) AS event_seq_filled
    FROM latest
    WHERE delivered_date >= DATE '2023-07-14'
      AND carrier IS NOT NULL
),
latest_shipments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC NULLS FIRST, id ASC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
metrics_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        next_cost,
        cost_bucket,
        event_seq_filled,
        COUNT(cost_bucket) OVER (PARTITION BY carrier ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost_bucket,
        SUM(cost_bucket) OVER (PARTITION BY carrier ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost_bucket,
        MAX(cost_bucket) OVER (PARTITION BY carrier ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost_bucket,
        AVG(cost_bucket) OVER (PARTITION BY carrier ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost_bucket
    FROM latest_shipments
)
-- Grouped metrics
SELECT
    CASE WHEN cost_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS cost_bucket_tier,
    ROUND(AVG(running_count_cost_bucket), 2) AS avg_running_count_cost_bucket,
    ROUND(AVG(running_sum_cost_bucket), 2) AS avg_running_sum_cost_bucket,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count_2
FROM metrics_shipments
GROUP BY cost_bucket_tier
ORDER BY avg_running_sum_cost_bucket DESC NULLS FIRST;
