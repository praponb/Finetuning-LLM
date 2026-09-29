WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(amount) AS amount_whole,
        CEIL(shipping_fee) AS shipping_fee_whole,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE order_date > DATE '2023-03-21'
)
-- Semi-join via a correlated subquery
SELECT s.*
FROM valid s
WHERE EXISTS (
        SELECT 1
        FROM payments c
        WHERE c.order_id = s.id
          AND c.paid_at >= s.order_date
    );
