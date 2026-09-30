WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM shipments
    EXCEPT
    SELECT id
    FROM shipments
    WHERE shipped_date BETWEEN '2024-03-15' AND '2025-09-07'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CUME_DIST() OVER w_running AS cost_cume_dist,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        DENSE_RANK() OVER w_recent AS cost_dense_rank,
        PERCENT_RANK() OVER w_ordered AS cost_percent_rank,
        CUME_DIST() OVER w_running AS cost_cume_dist_2
    FROM verified
    WINDOW w_running AS (PARTITION BY order_id ORDER BY cost ASC NULLS FIRST), w_recent AS (PARTITION BY order_id, status ORDER BY cost ASC NULLS FIRST), w_ordered AS (ORDER BY cost DESC NULLS LAST)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        cost,
        cost_share,
        cost_percent_rank,
        RANK() OVER w_part AS cost_percent_rank_rank,
        COUNT(cost) OVER w_hist AS order_id_count_cost,
        MIN(cost_cume_dist) OVER (PARTITION BY carrier) AS carrier_min_cost_cume_dist,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost_cume_dist_2 DESC) AS cost_cume_dist_2_dense_rank,
        CUME_DIST() OVER (PARTITION BY order_id, carrier ORDER BY cost_cume_dist DESC NULLS FIRST) AS cost_cume_dist_cume_dist
    FROM metrics
    WINDOW w_part AS (PARTITION BY carrier ORDER BY cost_percent_rank DESC NULLS LAST), w_hist AS (PARTITION BY order_id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY cost_percent_rank ASC) AS cost_percent_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY order_id_count_cost DESC NULLS LAST) AS order_id_count_cost_dense_rank,
        RANK() OVER (ORDER BY cost_percent_rank ASC NULLS LAST) AS cost_percent_rank_rank_2
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost_percent_rank_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS cost_percent_rank_dense_rank_tier,
    CASE WHEN order_id_count_cost >= 13 THEN 'LARGE' ELSE 'SMALL' END AS order_id_count_cost_tier,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2
FROM with_windows
GROUP BY CASE WHEN cost_percent_rank_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN order_id_count_cost >= 13 THEN 'LARGE' ELSE 'SMALL' END
HAVING cnt_2 >= 1
ORDER BY cost_percent_rank_dense_rank_tier ASC;
