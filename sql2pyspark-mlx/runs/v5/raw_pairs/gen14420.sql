WITH base AS (
    SELECT *, LAST_DAY(delivered_date) AS delivered_date_month_end
    FROM shipments
    WHERE delivered_date > DATE '2023-08-21'
      AND status IN ('LOST', 'IN_TRANSIT', 'RETURNED')
),
joined AS (
    SELECT src.*, b.discount
    FROM base src
    JOIN orders b
        ON src.order_id <=> b.id
    WHERE b.channel IN ('web', 'partner', 'store')
),
joined_shipments AS (
    SELECT a.*, b.status AS order_status
    FROM joined a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
enriched AS (
    SELECT
        *,
        LEAD(cost, 2, 0) OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY order_status, status ORDER BY delivered_date_month_end, id) AS row_seq,
        AVG(cost) OVER (ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior,
        RANK() OVER (PARTITION BY order_id ORDER BY discount DESC NULLS LAST) AS discount_rank,
        MIN(cost) OVER (PARTITION BY order_status) AS order_status_min_cost
    FROM joined_shipments
),
summary AS (
    SELECT
        CASE WHEN order_status_min_cost >= 106 THEN 'HIGH' ELSE 'LOW' END AS order_status_min_cost_tier,
        order_status,
        status,
        COUNT_IF(order_status = 'RETURNED') AS returned_events,
        SUM(discount) FILTER (WHERE carrier = 'Flash') AS flash_total_discount,
        SIZE(COLLECT_SET(status)) AS num_status,
        SIZE(COLLECT_SET(status)) AS num_status_2,
        COUNT(*) AS event_count,
        AVG(cost) FILTER (WHERE status = 'LOST') AS lost_avg_cost
    FROM enriched
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 1
)
SELECT
    order_status_min_cost_tier,
    order_status,
    status,
    returned_events,
    flash_total_discount,
    num_status,
    num_status_2,
    event_count,
    lost_avg_cost
FROM summary
ORDER BY order_status DESC;
