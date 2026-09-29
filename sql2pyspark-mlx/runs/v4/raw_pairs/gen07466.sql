WITH base AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label
    FROM stores
    WHERE opened_date >= DATE '2023-09-18'
      AND region IS NOT NULL
      AND city NOT IN ('Berlin', 'Singapore')
),
scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        opened_date_label,
        LAST_VALUE(region, TRUE) OVER (PARTITION BY region, city ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_region,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC) row_seq
    FROM base
),
with_last AS (
    SELECT s.*, x.order_order_date
    FROM scored s
    JOIN LATERAL (
            SELECT c.order_date AS order_order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    SELECT
        s.*,
        x.channel,
        x.order_shipping_fee
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.channel, c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    opened_date_label,
    last_known_region,
    order_order_date,
    order_shipping_fee,
    CASE WHEN city RLIKE '^(London|Bangkok)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS city_zone
FROM with_latest
ORDER BY city_zone DESC NULLS FIRST;
