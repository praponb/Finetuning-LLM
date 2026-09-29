WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(discount) AS discount_whole,
        LENGTH(channel) AS channel_length,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM orders
    WHERE order_date >= DATE '2024-01-17'
      AND store_id IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    CORR(amount, shipping_fee) OVER w_running AS corr_amount_shipping_fee,
    AVG(discount) OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_avg_5,
    SUM(discount_whole) OVER (PARTITION BY channel, customer_id ORDER BY channel_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_whole,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS seq_no,
    SUM(channel_length) OVER w_running AS status_sum_channel_length
FROM base
WINDOW w_running AS (PARTITION BY status)
