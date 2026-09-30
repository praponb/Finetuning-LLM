WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND customer_id IS NULL
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
eligible AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.quantity < 7
        )
),
enriched AS (
    SELECT
        s.*,
        b.country,
        b.name
    FROM eligible s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
    WHERE b.name LIKE 'FR%'
),
metrics AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        discount,
        days_since_order_date,
        row_num,
        name,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) AS event_seq,
        LEAD(shipping_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY row_num, id) AS next_shipping_fee,
        SUM(days_since_order_date) OVER (PARTITION BY store_id, channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_order_date
    FROM enriched
),
enriched_orders AS (
    SELECT s.*, b.sqft
    FROM metrics s
    LEFT JOIN stores b
        ON s.store_id = b.id
)
SELECT
    id,
    order_date,
    discount,
    days_since_order_date,
    row_num,
    name,
    event_seq,
    next_shipping_fee,
    cumulative_days_since_order_date
FROM enriched_orders
