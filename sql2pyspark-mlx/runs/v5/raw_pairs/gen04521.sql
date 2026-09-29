WITH cleaned AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), order_date) days_since_order_date
    FROM orders
    WHERE order_date >= DATE '2023-04-20'
),
with_latest AS (
    SELECT s.*, x.shipment_cost
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    SELECT
        *,
        LEAD(shipping_fee) OVER w_running next_shipping_fee,
        LAG(shipping_fee) OVER w_running prev_shipping_fee,
        LEAD(shipment_cost) OVER w_running next_shipment_cost,
        ROW_NUMBER() OVER w_running row_seq,
        LAG(shipping_fee, 2) OVER (ORDER BY order_date, id) prev_shipping_fee_2
    FROM with_latest
    WINDOW w_running AS (PARTITION BY status ORDER BY amount, id)
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.order_date,
        a.status,
        a.amount,
        a.shipping_fee,
        a.days_since_order_date,
        a.prev_shipping_fee,
        a.next_shipment_cost,
        a.prev_shipping_fee_2,
        p.country
    FROM scored a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
valid AS (
    SELECT
        id,
        customer_id,
        order_date,
        status,
        days_since_order_date,
        prev_shipping_fee,
        next_shipment_cost,
        prev_shipping_fee_2,
        country,
        CASE WHEN prev_shipping_fee >= 40 THEN 'High' WHEN prev_shipping_fee >= 11 THEN 'Medium' ELSE 'Low' END prev_shipping_fee_band,
        COALESCE(country, 'OTHER') resolved_country
    FROM enriched
    WHERE country IS NOT NULL
      AND customer_id IN (4, 9, 12)
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.days_since_order_date,
    s.prev_shipping_fee,
    s.next_shipment_cost,
    s.prev_shipping_fee_2,
    s.prev_shipping_fee_band,
    s.resolved_country,
    (SELECT MAX(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS max_account_balance
FROM valid s
ORDER BY days_since_order_date DESC NULLS FIRST
