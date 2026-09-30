WITH cleaned AS (
    SELECT
        *,
        CEIL(discount) AS discount_whole,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE order_date >= DATE '2023-06-23'
      AND channel IS NOT NULL
      AND store_id IN (9, 17, 28, 39)
),
latest AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY discount DESC NULLS FIRST, id) = 1
),
filtered AS (
    SELECT
        *,
        CASE WHEN shipping_fee >= 31 THEN 'High' WHEN shipping_fee >= 20 THEN 'Medium' ELSE 'Low' END AS shipping_fee_level,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM latest
    WHERE order_date > DATE '2023-05-10'
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM filtered src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    id,
    order_date,
    status,
    discount,
    resolved_channel,
    promo_id,
    discount_pct,
    CASE WHEN status RLIKE '^(RETURNED|SHIPPED)$' THEN 'EXTENDED' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'GROUP_B' ELSE 'CORE' END AS status_zone
FROM matched
ORDER BY order_date DESC, id DESC
