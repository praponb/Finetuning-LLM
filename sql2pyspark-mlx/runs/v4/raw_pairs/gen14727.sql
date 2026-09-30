WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(order_date, 'yyyy-MM-dd') AS order_date_label,
        ROUND(amount * 100, 2) AS amount_adjusted
    FROM orders
    WHERE order_date >= TO_DATE('2023-07-10')
      AND channel IS NOT NULL
      AND order_date > '2024-09-09'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS shipping_fee_cume_dist,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY store_id, channel), 4) AS amount_share,
        MAX(amount_adjusted) OVER w_grp AS channel_max_amount_adjusted,
        COUNT(shipping_fee) OVER w_grp AS channel_count_shipping_fee
    FROM valid
    WINDOW w_ordered AS (PARTITION BY channel, customer_id ORDER BY shipping_fee NULLS FIRST), w_grp AS (PARTITION BY channel)
),
valid_orders AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN last_known_amount >= 1680 THEN 'High' WHEN last_known_amount >= 350 THEN 'Medium' ELSE 'Low' END AS last_known_amount_band
    FROM enriched
    WHERE customer_id IS NOT NULL
)
SELECT
    id,
    customer_id,
    store_id,
    status,
    channel,
    discount,
    shipping_fee,
    order_date_label,
    shipping_fee_cume_dist,
    CASE WHEN status RLIKE '^(RETURNED|CANCELLED)$' THEN 'EXTENDED' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'GROUP_A' ELSE 'CORE' END AS status_zone
FROM valid_orders
ORDER BY id
