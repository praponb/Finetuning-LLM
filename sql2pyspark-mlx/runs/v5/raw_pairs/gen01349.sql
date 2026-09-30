WITH filtered AS (
    SELECT *, DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date
    FROM shipments
    WHERE order_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        days_shipped_date_to_delivered_date,
        CUME_DIST() OVER (ORDER BY cost NULLS LAST) AS cost_cume_dist,
        LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_cost,
        COUNT(days_shipped_date_to_delivered_date) OVER (PARTITION BY order_id) AS order_id_count_days_shipped_date_to_delivered_date,
        LAG(cost, 2) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_cost_2,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS FIRST) AS cost_percent_rank
    FROM filtered
),
filtered_shipments AS (
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        days_shipped_date_to_delivered_date,
        cost_cume_dist,
        prev_cost,
        prev_cost_2,
        cost_percent_rank,
        ROUND(cost_percent_rank * 1.15, 2) AS adjusted_cost_percent_rank,
        LEAST(cost_cume_dist, order_id_count_days_shipped_date_to_delivered_date) AS least_cost_cume_dist_order_id_count_days_shipped_date_to_delivered_date
    FROM scored
    WHERE carrier IS NOT NULL
      AND delivered_date > DATE '2023-09-18'
),
joined AS (
    SELECT e.*, d.channel
    FROM filtered_shipments e
    INNER JOIN orders d
        ON e.order_id = d.id
)
SELECT
    id,
    order_id,
    kpi,
    value
FROM (
    SELECT
        id,
        order_id,
        prev_cost,
        adjusted_cost_percent_rank
    FROM joined
) src
UNPIVOT (
    value FOR kpi IN (prev_cost, adjusted_cost_percent_rank)
);
