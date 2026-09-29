WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(discount) AS discount_whole,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE customer_id IS NOT NULL
      AND amount < 960
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    channel,
    metric,
    reading
FROM (
    SELECT
        id,
        channel,
        amount,
        shipping_fee
    FROM cleaned
) src
UNPIVOT (
    reading FOR metric IN (amount, shipping_fee)
);
