WITH numbered AS (
    SELECT
        id,
        customer_id,
        shipping_fee,
        amount,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) rnk
    FROM orders
)
SELECT
    id,
    customer_id,
    shipping_fee,
    amount
FROM numbered
WHERE rnk <= 1
ORDER BY customer_id DESC
