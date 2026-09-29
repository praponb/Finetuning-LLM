WITH with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
eligible AS (
    SELECT s.*
    FROM with_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (8, 38)
        )
),
with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.amount,
        s.shipping_fee,
        s.promo_id,
        top1.method,
        top1.payment_paid_at
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.method, c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.order_date,
        a.promo_id,
        a.method,
        p.signup_date,
        p.name,
        p.city
    FROM with_latest a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_latest_orders AS (
    SELECT s.*, x.unit_price
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    p.country,
    s.city AS order_city,
    COUNT(*) AS rows_total,
    MAX(s.unit_price) AS max_unit_price,
    MIN(s.promo_id) AS min_promo_id,
    GROUPING_ID() AS level_id
FROM with_latest_orders s
LEFT JOIN customers p ON s.customer_id = p.id
GROUP BY GROUPING SETS ((p.country, s.city), (p.country), ())
ORDER BY rows_total NULLS LAST
