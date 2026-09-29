WITH cleaned AS (
    SELECT
        *,
        CONCAT_WS('/', channel, status) AS channel_status_key,
        CONCAT(status, ', ', channel) AS status_channel
    FROM orders
    WHERE order_date > TO_DATE('2023-06-01')
      AND channel IS DISTINCT FROM 'web'
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee ASC NULLS LAST) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER w_recent AS last_known_shipping_fee,
        RANK() OVER (ORDER BY shipping_fee ASC NULLS FIRST) AS shipping_fee_rank,
        AVG(amount) OVER (PARTITION BY store_id ORDER BY order_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
        CORR(amount, shipping_fee) OVER w_running AS corr_amount_shipping_fee,
        ROUND(amount / SUM(amount) OVER (PARTITION BY channel), 4) AS amount_share
    FROM deduped
    WHERE shipping_fee >= 30
    WINDOW w_recent AS (ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY customer_id)
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id = 31
        )
),
with_windows AS (
    SELECT
        *,
        LAG(amount_avg_5_prior) OVER w_part AS prev_amount_avg_5_prior,
        LEAD(shipping_fee) OVER w_part AS next_shipping_fee,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY store_id ORDER BY corr_amount_shipping_fee ASC, id ASC)
),
kept_keys AS (
    SELECT store_id
    FROM with_windows
    EXCEPT
    SELECT store_id
    FROM orders
    WHERE shipping_fee < 35
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.store_id = k.store_id
)
SELECT
    customer_id,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    APPROX_PERCENTILE(shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_quartiles,
    COUNT_IF(status = 'SHIPPED') AS shipped_events,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(discount, 0.9) AS discount_p90,
    SUM(CASE WHEN channel = 'web' THEN corr_amount_shipping_fee ELSE 0 END) AS web_corr_amount_shipping_fee
FROM verified
GROUP BY customer_id, DATE_TRUNC('QUARTER', order_date)
HAVING web_corr_amount_shipping_fee > 0
ORDER BY shipped_events ASC
