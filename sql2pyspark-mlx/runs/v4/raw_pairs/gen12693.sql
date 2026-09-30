WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS dedup_rank
    FROM orders
    QUALIFY dedup_rank = 1
),
with_latest AS (
    SELECT
        s.*,
        top1.shipment_status,
        top1.shipment_cost
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status, c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        dedup_rank,
        shipment_status,
        shipment_cost,
        shipping_fee + amount AS amount_with_shipping
    FROM with_latest
    WHERE order_date >= DATE '2023-08-14'
)
SELECT
    customer_id,
    COUNT(*) FILTER (WHERE shipment_status = 'DELIVERED') AS delivered_rows,
    COUNT(CASE WHEN shipment_status = 'DELIVERED' THEN 1 END) AS delivered_count,
    SUM(CASE WHEN status = 'COMPLETED' THEN discount ELSE 0 END) AS completed_discount
FROM cleaned
GROUP BY customer_id
