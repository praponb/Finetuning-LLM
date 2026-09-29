WITH cleaned AS (
    SELECT *, INITCAP(TRIM(channel)) AS channel_title
    FROM orders
    WHERE channel IS NOT DISTINCT FROM 'store'
      AND order_date >= DATE '2023-08-08'
),
latest_match AS (
    SELECT
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.channel_title,
        top1.payment_method,
        top1.payment_status
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.method AS payment_method, c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    SELECT
        s.id,
        s.status,
        s.channel,
        s.discount,
        s.payment_method,
        s.payment_status,
        latest.payment_paid_at,
        latest.payment_amount
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at, c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.status,
        s.discount,
        s.payment_method,
        s.payment_paid_at,
        s.payment_amount,
        top1.shipment_status
    FROM with_last s
    JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.payment_paid_at
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
cleaned_orders AS (
    SELECT
        id,
        status,
        discount,
        payment_method,
        CASE WHEN shipment_status IN ('RETURNED', 'DELIVERED') THEN 'Group A' ELSE 'Other' END AS shipment_status_group,
        COALESCE(payment_method, 'UNKNOWN') AS payment_method_clean
    FROM with_latest
    WHERE shipment_status IS NOT NULL
      AND shipment_status IS DISTINCT FROM 'RETURNED'
),
with_windows AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY payment_method ORDER BY discount DESC NULLS FIRST) AS discount_rank,
        AVG(discount) OVER (PARTITION BY payment_method) AS payment_method_avg_discount
    FROM cleaned_orders
)
SELECT
    id,
    status,
    discount,
    payment_method,
    shipment_status_group,
    payment_method_clean,
    discount_rank,
    payment_method_avg_discount,
    CASE WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS status_zone
FROM with_windows;
