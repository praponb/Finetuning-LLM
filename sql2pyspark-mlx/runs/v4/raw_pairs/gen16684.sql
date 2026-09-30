WITH ranked AS (
    SELECT *
    FROM orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY shipping_fee NULLS LAST, id) = 1
)
SELECT
    id,
    customer_id,
    store_id,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    CASE WHEN status RLIKE '^(CANCELLED|RETURNED)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS status_zone
FROM ranked
ORDER BY channel, status;
