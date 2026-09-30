WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, channel ORDER BY discount DESC, id ASC) AS latest_rank
    FROM orders
    QUALIFY latest_rank = 1
)
SELECT
    id,
    order_date,
    status,
    channel,
    discount,
    shipping_fee,
    latest_rank
FROM deduped
ORDER BY latest_rank ASC, order_date ASC
