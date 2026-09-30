WITH cleaned AS (
    SELECT
        *,
        DATE_SUB(order_date, 14) order_date_minus_14d,
        CASE WHEN amount >= 1340 THEN 'High' WHEN amount >= 510 THEN 'Medium' ELSE 'Low' END amount_band,
        COALESCE(channel, 'UNKNOWN') channel_filled
    FROM orders
    WHERE customer_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        channel,
        status,
        shipping_fee
    FROM cleaned
) src
PIVOT (
    AVG(shipping_fee) AS amount,
    COUNT(1) AS cnt
    FOR status IN ('CANCELLED' AS cancelled, 'RETURNED' AS returned)
);
