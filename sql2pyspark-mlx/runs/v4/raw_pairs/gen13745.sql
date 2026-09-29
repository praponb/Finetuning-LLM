WITH base AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END AS is_dhl,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM shipments
    WHERE delivered_date >= DATE '2023-05-31'
      AND carrier IS NOT NULL
      AND shipped_date >= '2025-09-01'
),
scored AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        is_dhl,
        resolved_carrier,
        AVG(cost) OVER (PARTITION BY order_id, status ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        LAG(is_dhl, 1) OVER (ORDER BY cost, id) AS prev_is_dhl,
        SUM(cost) OVER w_grp AS running_cost
    FROM base
    WINDOW w_grp AS (ORDER BY is_dhl, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        order_id,
        cost,
        status,
        is_dhl,
        resolved_carrier,
        cost_avg_6,
        prev_is_dhl,
        running_cost,
        DENSE_RANK() OVER (ORDER BY cost_avg_6 NULLS LAST) AS cost_avg_6_dense_rank,
        FIRST_VALUE(cost) IGNORE NULLS OVER (ORDER BY prev_is_dhl, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        SUM(running_cost) OVER (PARTITION BY order_id, status) AS order_id_sum_running_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_dhl, id) AS seq_no,
        AVG(cost) OVER (ORDER BY is_dhl, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2
    FROM scored
),
valid AS (
    SELECT
        id,
        status,
        is_dhl,
        resolved_carrier,
        cost_avg_6,
        prev_is_dhl,
        cost_avg_6_dense_rank,
        first_known_cost,
        seq_no,
        cost_avg_2,
        ROUND(is_dhl * 0.9, 2) AS is_dhl_adjusted
    FROM enriched
    WHERE order_id IS NOT NULL
      AND cost > 67
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_dhl NULLS LAST, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
base_shipments AS (
    SELECT
        id,
        is_dhl,
        resolved_carrier,
        cost_avg_6,
        prev_is_dhl,
        cost_avg_6_dense_rank,
        first_known_cost,
        seq_no,
        cost_avg_2,
        is_dhl_adjusted,
        rank_in_group,
        is_dhl_adjusted + rank_in_group AS is_dhl_adjusted_plus_rank_in_group
    FROM latest
    WHERE prev_is_dhl BETWEEN 29 AND 57
),
summary AS (
    SELECT
        seq_no,
        APPROX_PERCENTILE(is_dhl, 0.9) AS is_dhl_p90,
        ROUND(AVG(cost_avg_6), 2) AS avg_cost_avg_6,
        PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
        ROUND(AVG(cost_avg_2), 2) AS avg_cost_avg_2
    FROM base_shipments
    GROUP BY 1
    HAVING is_dhl_p90 >= 0
)
SELECT
    seq_no,
    is_dhl_p90,
    seq_no_quartiles,
    avg_cost_avg_2
FROM summary
ORDER BY is_dhl_p90 DESC NULLS FIRST, avg_cost_avg_2;
