WITH with_last AS (
    SELECT s.*, x.payment_status
    FROM orders s
    JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    SELECT
        *,
        LEAD(discount) OVER w_grp AS next_discount,
        LEAD(shipping_fee) OVER w_grp AS next_shipping_fee,
        ROW_NUMBER() OVER w_grp AS row_seq,
        NTILE(5) OVER (PARTITION BY status ORDER BY shipping_fee, id) AS shipping_fee_bucket
    FROM with_last
    WHERE MONTH(order_date) IN (4, 9, 12)
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY amount, id)
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_shipping_fee, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
latest_match AS (
    SELECT s.*, latest.shipment_status
    FROM deduped s
    JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched_orders AS (
    SELECT
        id,
        channel,
        discount,
        next_discount,
        next_shipping_fee,
        row_seq,
        shipping_fee_bucket,
        dedup_rank,
        SUM(row_seq) OVER (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        NTILE(4) OVER (PARTITION BY payment_status ORDER BY shipping_fee_bucket DESC, id) AS shipping_fee_bucket_bucket,
        NTILE(4) OVER w_running AS amount_bucket
    FROM latest_match
    WINDOW w_running AS (PARTITION BY channel ORDER BY amount, id)
)
SELECT
    channel,
    COUNT_IF(channel = 'partner') AS partner_events,
    ROUND(SUM(amount_bucket) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS amount_bucket_per_partner,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    SUM(next_discount) AS total_next_discount,
    SUM(shipping_fee_bucket_bucket) AS total_shipping_fee_bucket_bucket,
    GROUPING_ID() AS grouping_level
FROM enriched_orders
GROUP BY CUBE (channel)
HAVING ROUND(SUM(amount_bucket) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) > 0
