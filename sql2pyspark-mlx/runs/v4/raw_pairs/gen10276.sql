WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount, id) AS row_num
        FROM orders
    ) t
    WHERE row_num = 1
),
eligible AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at >= s.order_date
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id IN (11, 20, 25)
        )
)
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
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'CORE' WHEN channel RLIKE '^(web|mobile)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS channel_zone
FROM not_flagged
ORDER BY status DESC NULLS FIRST
