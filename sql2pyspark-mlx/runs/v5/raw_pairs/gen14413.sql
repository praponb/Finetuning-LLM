WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN status IN ('LOST', 'DELIVERED') THEN 'Tier 1' ELSE 'Other' END AS status_group,
        LAST_DAY(delivered_date) AS delivered_date_month_end,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT carrier, id
    FROM cleaned
    INTERSECT
    SELECT carrier, id
    FROM shipments
    WHERE delivered_date <= '2023-06-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.carrier = k.carrier AND s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        status_group,
        delivered_date_month_end,
        RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) AS cost_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        MIN(cost) OVER (PARTITION BY order_id) AS order_id_min_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share_2
    FROM reconciled
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier,
        s.shipped_date,
        s.delivered_date,
        s.status,
        s.status_group,
        s.order_id_min_cost,
        d.status AS order_status,
        d.channel,
        d.amount
    FROM scored s
    JOIN orders d
        ON s.order_id <=> d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(order_id_min_cost) OVER (PARTITION BY channel) AS channel_count_order_id_min_cost,
        ROUND(amount / SUM(amount) OVER (PARTITION BY channel), 4) AS amount_share,
        DENSE_RANK() OVER (PARTITION BY order_status ORDER BY order_id_min_cost DESC NULLS FIRST) AS order_id_min_cost_dense_rank
    FROM enriched
    WHERE YEAR(delivered_date) >= 2025
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
        COUNT(*) AS row_count,
        AVG(channel_count_order_id_min_cost) AS avg_channel_count_order_id_min_cost,
        PERCENTILE_APPROX(amount, 0.9) AS amount_p90,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM metrics
    GROUP BY DATE_TRUNC('QUARTER', delivered_date)
)
SELECT
    quarter_start,
    row_count,
    avg_channel_count_order_id_min_cost,
    amount_p90,
    num_status,
    distinct_channels
FROM daily
ORDER BY quarter_start DESC;
