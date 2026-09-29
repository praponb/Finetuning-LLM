WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CASE WHEN amount >= 1270 THEN 'High' WHEN amount >= 230 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        GREATEST(amount, shipping_fee) AS greatest_amount_shipping_fee
    FROM orders
    WHERE order_date >= DATE '2023-08-18'
      AND status <> 'SHIPPED'
)
SELECT
    s.*,
    latest.shipment_carrier,
    latest.shipment_delivered_date
FROM valid s
LEFT JOIN LATERAL (
        SELECT c.carrier AS shipment_carrier, c.delivered_date AS shipment_delivered_date
        FROM shipments c
        WHERE c.order_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
