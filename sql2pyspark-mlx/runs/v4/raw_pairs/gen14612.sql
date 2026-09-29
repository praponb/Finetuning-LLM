WITH valid AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        GREATEST(shipping_fee, amount) AS greatest_shipping_fee_amount
    FROM orders
    WHERE order_date > TO_DATE('2023-11-12')
      AND customer_id IS NOT NULL
)
SELECT s.*, latest.shipment_delivered_date
FROM valid s
LEFT JOIN LATERAL (
        SELECT c.delivered_date AS shipment_delivered_date
        FROM shipments c
        WHERE c.order_id = s.id
        ORDER BY c.delivered_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
