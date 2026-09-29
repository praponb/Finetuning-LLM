WITH with_latest AS (
    SELECT
        s.*,
        x.shipment_status,
        x.shipment_delivered_date
    FROM orders s
    JOIN LATERAL (
            SELECT c.status AS shipment_status, c.delivered_date AS shipment_delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    shipment_status,
    DATE_TRUNC('WEEK', shipment_delivered_date) AS week_start,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95,
    ROUND(AVG(amount), 2) AS avg_amount,
    SIZE(COLLECT_SET(shipment_status)) AS num_shipment_status,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    SORT_ARRAY(COLLECT_SET(shipment_status)) AS shipment_status_list,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END), 0), 2) AS amount_per_cancelled
FROM with_latest
GROUP BY shipment_status, DATE_TRUNC('WEEK', shipment_delivered_date)
ORDER BY week_start NULLS FIRST;
