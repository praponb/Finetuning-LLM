WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee) AS rnk
    FROM orders
)
SELECT id, store_id, shipping_fee
FROM numbered
WHERE rnk <= 3
ORDER BY store_id DESC, id DESC
