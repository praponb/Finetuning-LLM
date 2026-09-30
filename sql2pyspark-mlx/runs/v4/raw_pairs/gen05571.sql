WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status,
        CASE WHEN status IN ('RETURNED', 'IN_TRANSIT') THEN 'Tier 1' ELSE 'Other' END AS status_group
    FROM shipments
    WHERE delivered_date > TO_DATE('2024-03-06')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC NULLS FIRST, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(cost, 1) OVER (PARTITION BY order_id ORDER BY cost, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) AS event_seq,
        COUNT(cost) OVER (PARTITION BY status, order_id) AS status_count_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, status), 4) AS cost_share
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY next_cost NULLS LAST) AS next_cost_dense_rank,
        LAG(cost_share, 2) OVER w_recent AS prev_cost_share,
        SUM(cost) OVER (PARTITION BY order_id, status ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        NTILE(5) OVER (PARTITION BY status ORDER BY status_count_cost DESC, id) AS status_count_cost_bucket,
        RANK() OVER (PARTITION BY order_id ORDER BY status_count_cost NULLS FIRST) AS status_count_cost_rank
    FROM scored
    WINDOW w_recent AS (PARTITION BY status ORDER BY delivered_date, id)
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, p.status AS order_status
    FROM metrics src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
enriched_shipments AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.discount,
        b.status AS order_status_2,
        b.amount
    FROM enriched e
    JOIN orders b
        ON e.order_id = b.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    status,
    APPROX_PERCENTILE(cumulative_cost, 0.9) AS cumulative_cost_p90,
    SIZE(COLLECT_SET(status)) AS num_status,
    AVG(cumulative_cost) AS avg_cumulative_cost,
    SUM(CASE WHEN order_status_2 = 'COMPLETED' THEN amount ELSE 0 END) AS completed_amount,
    APPROX_PERCENTILE(next_cost_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS next_cost_dense_rank_quartiles
FROM enriched_shipments
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', delivered_date), status), (DATE_TRUNC('QUARTER', delivered_date)))
ORDER BY avg_cumulative_cost NULLS FIRST
