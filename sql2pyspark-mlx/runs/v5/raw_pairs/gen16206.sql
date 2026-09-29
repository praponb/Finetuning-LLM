WITH with_latest AS (
    SELECT s.*, x.shipment_status
    FROM orders s
    JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    SELECT
        *,
        ROUND(amount, 0) AS amount_rounded,
        INITCAP(TRIM(channel)) AS channel_title
    FROM with_latest
    WHERE order_date > DATE '2023-06-05'
),
latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.discount,
        s.shipment_status,
        s.channel_title,
        latest.delivered_date
    FROM filtered s
    JOIN LATERAL (
            SELECT c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.order_date
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    SELECT
        *,
        LEAD(discount) OVER (PARTITION BY customer_id, shipment_status ORDER BY delivered_date, id) AS next_discount,
        LAG(discount) OVER (PARTITION BY customer_id, shipment_status ORDER BY delivered_date, id) AS prev_discount,
        ROW_NUMBER() OVER (PARTITION BY customer_id, shipment_status ORDER BY delivered_date, id) AS seq_no,
        SUM(discount) OVER (PARTITION BY shipment_status ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount
    FROM latest_match
    WHERE discount > 0.36
),
valid AS (
    SELECT
        *,
        prev_discount * discount AS prev_discount_times_discount,
        COALESCE(shipment_status, 'UNKNOWN') AS shipment_status_filled
    FROM with_windows
    WHERE cumulative_discount > 1
),
kept AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.delivered_date < s.delivered_date
        )
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.discount,
    s.shipment_status,
    s.channel_title,
    s.next_discount,
    s.prev_discount,
    s.seq_no,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM kept s
ORDER BY discount DESC NULLS FIRST;
