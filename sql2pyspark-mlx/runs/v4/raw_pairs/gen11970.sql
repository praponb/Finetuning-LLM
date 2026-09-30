WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(carrier) AS clean_carrier,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM shipments
    WHERE YEAR(shipped_date) = 2025
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY carrier ORDER BY delivered_date NULLS LAST) = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, status
    FROM ranked
    INTERSECT ALL
    SELECT order_id, status
    FROM shipments
    WHERE carrier = 'Flash'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost) AS cost_percent_rank,
        DENSE_RANK() OVER (ORDER BY cost) AS cost_dense_rank,
        DENSE_RANK() OVER (PARTITION BY status, order_id ORDER BY cost DESC) AS cost_dense_rank_2,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share_2
    FROM verified
),
with_windows_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost_share_2) OVER w_part AS order_id_avg_cost_share_2,
        COUNT(cost_share_2) OVER (PARTITION BY status) AS status_count_cost_share_2,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY cost_dense_rank NULLS LAST) AS cost_dense_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_percent_rank_2
    FROM with_windows
    WINDOW w_part AS (PARTITION BY order_id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY cost) AS cost_cume_dist,
        AVG(cost_dense_rank_2) OVER (PARTITION BY carrier) AS carrier_avg_cost_dense_rank_2,
        COUNT(cost_percent_rank) OVER (PARTITION BY status, order_id) AS status_count_cost_percent_rank,
        CUME_DIST() OVER (PARTITION BY status, order_id ORDER BY order_id_avg_cost_share_2 DESC NULLS LAST) AS order_id_avg_cost_share_2_cume_dist
    FROM with_windows_shipments
),
with_dims AS (
    -- Attach orders attributes
    SELECT a.*, p.channel
    FROM enriched a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
grouped AS (
    -- Grouped metrics
    SELECT
        order_id,
        ROUND(SUM(cost_share) / NULLIF(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END), 0), 2) AS cost_share_per_delivered,
        ROUND(SUM(cost_dense_rank_2) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS cost_dense_rank_2_per_store,
        SUM(cost_percent_rank) AS total_cost_percent_rank,
        MAX(order_id_avg_cost_share_2) AS max_order_id_avg_cost_share_2,
        APPROX_PERCENTILE(order_id_avg_cost_share_2, 0.9) AS order_id_avg_cost_share_2_p90,
        COUNT_IF(carrier = 'FedEx') AS fedex_events
    FROM with_dims
    GROUP BY order_id
    HAVING fedex_events > 1
)
SELECT
    order_id,
    cost_dense_rank_2_per_store,
    total_cost_percent_rank,
    max_order_id_avg_cost_share_2,
    fedex_events
FROM grouped
