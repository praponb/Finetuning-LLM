WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_date,
        status,
        amount,
        shipping_fee,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        CONCAT(status, ' ', channel) AS status_channel,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date > DATE '2023-09-19'
      AND customer_id IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *, RANK() OVER (PARTITION BY status ORDER BY days_since_order_date ASC NULLS LAST) AS rank_in_group
FROM cleaned
QUALIFY rank_in_group = 1;
