SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    NTILE(5) OVER w_ordered AS shipping_fee_bucket,
    NTILE(10) OVER (PARTITION BY channel ORDER BY discount ASC, id ASC) AS discount_bucket
FROM orders
WHERE amount IS NULL
WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY shipping_fee ASC, id ASC);
