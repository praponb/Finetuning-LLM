WITH valid AS (
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
        TRIM(channel) AS clean_channel,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM orders
    WHERE order_date >= DATE '2023-08-08'
      AND store_id IS NOT NULL
),
enriched AS (
    SELECT
        s.*,
        p.credit_limit,
        p.country
    FROM valid s
    JOIN customers p
        ON s.customer_id = p.id
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.order_id = 36
        )
),
latest_match AS (
    SELECT s.*, x.delivered_date
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible_orders AS (
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.status = 'IN_TRANSIT'
        )
),
scored AS (
    SELECT
        *,
        AVG(discount) OVER w_part AS discount_avg_2_prior,
        SUM(shipping_fee) OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee,
        LEAD(amount, 1) OVER (ORDER BY credit_limit, id) AS next_amount,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        AVG(discount) OVER w_recent AS discount_avg_2
    FROM eligible_orders
    WINDOW w_part AS (PARTITION BY country, store_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_recent AS (ORDER BY shipping_fee, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    customer_id,
    store_id,
    channel,
    discount,
    shipping_fee,
    clean_channel,
    channel_filled,
    country,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'GROUP_B' WHEN channel RLIKE '^(mobile|partner)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS channel_zone
FROM scored;
