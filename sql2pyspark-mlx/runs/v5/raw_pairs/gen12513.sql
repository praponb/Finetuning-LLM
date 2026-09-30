WITH valid AS (
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        LOWER(status) status_lower
    FROM orders
    WHERE customer_id IS NOT NULL
      AND discount >= 0.22
)
SELECT channel, status
FROM valid
EXCEPT ALL
SELECT channel, status
FROM orders
WHERE amount < 1130;
