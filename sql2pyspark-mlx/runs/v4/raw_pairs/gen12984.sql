WITH cleaned AS (
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        DATE_FORMAT(order_date, 'MMM yyyy') order_date_label,
        COALESCE(channel, 'N/A') resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-06-26'
)
SELECT *
FROM (
    SELECT
        store_id,
        status,
        channel,
        amount
    FROM cleaned
) src
PIVOT (
    AVG(amount) AS total,
    COUNT(1) AS cnt
    FOR channel IN ('store', 'mobile')
);
