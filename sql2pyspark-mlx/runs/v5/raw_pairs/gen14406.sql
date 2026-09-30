WITH filtered AS (
    SELECT
        *,
        LAST_DAY(order_date) AS order_date_month_end,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM orders
    WHERE order_date > TO_DATE('2023-07-22')
      AND amount > 510
)
SELECT *
FROM (
    SELECT
        store_id,
        status,
        shipping_fee
    FROM filtered
) src
PIVOT (
    MAX(shipping_fee)
    FOR status IN ('COMPLETED' AS completed, 'CANCELLED' AS cancelled, 'SHIPPED' AS shipped)
)
