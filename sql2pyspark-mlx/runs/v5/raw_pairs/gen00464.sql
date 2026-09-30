WITH kept_keys AS (
    -- Set intersection on keys
    SELECT channel, id
    FROM orders
    INTERSECT ALL
    SELECT channel, id
    FROM orders
    WHERE store_id = 38
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM orders s
    INNER JOIN kept_keys k ON s.channel = k.channel AND s.id = k.id
)
SELECT
    s.customer_id,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    (SELECT AVG(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_shipping_fee
FROM reconciled s;
