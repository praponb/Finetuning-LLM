WITH matched_keys AS (
    -- Set difference on keys
    SELECT carrier, order_id
    FROM shipments
    EXCEPT
    SELECT carrier, order_id
    FROM shipments
    WHERE order_id IN (23, 26, 33)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    INNER JOIN matched_keys k ON s.carrier = k.carrier AND s.order_id = k.order_id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        COUNT(cost) OVER (PARTITION BY order_id) AS order_id_count_cost,
        PERCENT_RANK() OVER w_recent AS cost_percent_rank
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY order_id, carrier ORDER BY cost ASC NULLS LAST)
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_cume_dist,
        order_id_count_cost,
        cost_percent_rank,
        RANK() OVER (ORDER BY order_id_count_cost DESC NULLS FIRST) AS order_id_count_cost_rank,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost_percent_rank DESC) AS cost_percent_rank_dense_rank
    FROM scored
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        order_id_count_cost,
        cost_percent_rank,
        order_id_count_cost_rank,
        cost * cost_percent_rank_dense_rank AS cost_times_cost_percent_rank_dense_rank
    FROM scored_shipments
    WHERE carrier IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN cost_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS cost_percent_rank_tier,
    CASE WHEN cost_times_cost_percent_rank_dense_rank >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_times_cost_percent_rank_dense_rank_tier,
    ROUND(AVG(cost), 2) AS avg_cost,
    SUM(cost) AS total_cost,
    MIN(cost_times_cost_percent_rank_dense_rank) AS min_cost_times_cost_percent_rank_dense_rank,
    ROUND(AVG(cost_times_cost_percent_rank_dense_rank), 2) AS avg_cost_times_cost_percent_rank_dense_rank,
    COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count,
    GROUPING_ID() AS grouping_level
FROM base
GROUP BY GROUPING SETS ((CASE WHEN cost_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cost_times_cost_percent_rank_dense_rank >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN cost_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING ROUND(AVG(cost), 2) < 0;
