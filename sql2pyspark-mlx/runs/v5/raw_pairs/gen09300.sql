WITH remaining_keys AS (
    SELECT id
    FROM shipments
    EXCEPT ALL
    SELECT id
    FROM shipments
    WHERE delivered_date >= '2023-11-18'
),
kept_rows AS (
    SELECT s.*
    FROM shipments s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    SELECT
        order_id,
        shipped_date,
        status,
        ROUND(cost * 1.1, 2) cost_adjusted,
        COALESCE(carrier, 'UNKNOWN') carrier_clean
    FROM kept_rows
    WHERE shipped_date > TO_DATE('2023-05-24')
      AND carrier IS NOT DISTINCT FROM 'UPS'
),
with_windows AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY order_id ORDER BY cost_adjusted DESC NULLS FIRST) cost_adjusted_rank,
        AVG(cost_adjusted) OVER (PARTITION BY status) status_avg_cost_adjusted
    FROM valid
),
with_windows_shipments AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY cost_adjusted_rank DESC NULLS FIRST) cost_adjusted_rank_cume_dist,
        CUME_DIST() OVER (PARTITION BY order_id, status ORDER BY cost_adjusted_rank DESC NULLS FIRST) cost_adjusted_rank_cume_dist_2
    FROM with_windows
),
metrics AS (
    SELECT
        *,
        SUM(cost_adjusted_rank_cume_dist) OVER (PARTITION BY order_id, status) order_id_sum_cost_adjusted_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY cost_adjusted_rank_cume_dist DESC NULLS LAST) cost_adjusted_rank_cume_dist_dense_rank,
        MAX(cost_adjusted_rank_cume_dist) OVER (PARTITION BY status, order_id) status_max_cost_adjusted_rank_cume_dist,
        CUME_DIST() OVER (PARTITION BY status, order_id ORDER BY cost_adjusted NULLS FIRST) cost_adjusted_cume_dist
    FROM with_windows_shipments
)
SELECT
    order_id,
    status,
    cost_adjusted,
    carrier_clean,
    cost_adjusted_rank,
    cost_adjusted_rank_cume_dist,
    cost_adjusted_rank_cume_dist_2,
    cost_adjusted_rank_cume_dist_dense_rank,
    status_max_cost_adjusted_rank_cume_dist
FROM metrics
ORDER BY cost_adjusted DESC NULLS LAST, status_max_cost_adjusted_rank_cume_dist;
