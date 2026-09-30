WITH cleaned AS (
    SELECT
        *,
        DATE_TRUNC('week', order_date) AS order_date_period,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-06-01'
      AND channel IS NOT NULL
      AND store_id = 16
)
SELECT *
FROM (
    SELECT
        channel,
        status,
        amount
    FROM cleaned
) src
PIVOT (
    AVG(amount) AS value_sum
    FOR status IN ('COMPLETED', 'PENDING')
);
