WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        CAST(amount AS INT) AS amount_int,
        CONCAT(channel, ', ', status) AS channel_status
    FROM orders
    WHERE order_date > DATE '2023-05-15'
      AND shipping_fee < 17
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM cleaned s
WHERE NOT EXISTS (
        SELECT 1
        FROM order_items x
        WHERE x.order_id = s.id
          AND x.product_id IN (1, 39)
    );
