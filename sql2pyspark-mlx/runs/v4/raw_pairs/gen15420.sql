WITH filtered AS (
    SELECT
        *,
        UPPER(carrier) AS carrier_upper,
        DATE_TRUNC('month', delivered_date) AS delivered_date_period
    FROM shipments
    WHERE order_id IS NOT NULL
),
with_dims AS (
    SELECT e.*, o.amount
    FROM filtered e
    INNER JOIN orders o
        ON e.order_id = o.id
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY carrier ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        SUM(amount) OVER w_ordered AS cumulative_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS row_seq
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(delivered_date_period) OVER (PARTITION BY order_id ORDER BY delivered_date_period, id) IS NULL
                 OR delivered_date_period > LAG(delivered_date_period) OVER (PARTITION BY order_id ORDER BY delivered_date_period, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST, id) AS rn
    FROM sessionized
    QUALIFY rn = 1
),
scored AS (
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY burst_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        SUM(amount) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM ranked
)
SELECT
    carrier,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    MIN(delivered_date_period) AS first_seen,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    COUNT_IF(status = 'IN_TRANSIT') AS in_transit_events,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    ROUND(AVG(last_known_cost), 2) AS avg_last_known_cost
FROM scored
GROUP BY carrier, DATE_TRUNC('WEEK', shipped_date)
