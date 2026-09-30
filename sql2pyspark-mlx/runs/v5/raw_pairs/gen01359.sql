WITH base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        ADD_MONTHS(order_date, 6) AS order_date_plus_months,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM orders
    WHERE channel != 'web'
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM base s
    JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.order_date_plus_months >= pr.starts_on
       AND s.order_date_plus_months < pr.ends_on + INTERVAL '7' DAY
),
deduped AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        days_since_order_date,
        order_date_plus_months,
        promo_id,
        discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, promo_code ORDER BY order_date DESC, id, promo_id) AS row_num
        FROM matched
    ) x
    WHERE row_num = 1
),
latest_match AS (
    SELECT
        s.id,
        s.store_id,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.days_since_order_date,
        s.order_date_plus_months,
        s.promo_id,
        top1.order_item_unit_price,
        top1.quantity
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    promo_id,
    channel,
    metric,
    reading
FROM (
    SELECT
        id,
        promo_id,
        channel,
        order_item_unit_price,
        discount,
        shipping_fee
    FROM latest_match
) src
UNPIVOT (
    reading FOR metric IN (order_item_unit_price, discount, shipping_fee)
)
