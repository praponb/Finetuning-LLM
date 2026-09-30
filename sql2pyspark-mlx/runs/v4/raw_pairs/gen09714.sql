WITH numbered AS (
    SELECT
        id,
        channel,
        discount,
        store_id,
        RANK() OVER (PARTITION BY channel ORDER BY discount DESC) AS rnk
    FROM orders
)
SELECT
    id,
    channel,
    discount,
    store_id
FROM numbered
WHERE rnk <= 5
ORDER BY id DESC
