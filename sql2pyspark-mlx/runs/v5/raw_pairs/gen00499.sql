WITH with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.amount,
        s.discount,
        s.shipping_fee,
        latest.paid_at
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM with_last
    INTERSECT ALL
    SELECT id, status
    FROM orders
    WHERE status <> 'CANCELLED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        store_id,
        order_date,
        status,
        discount,
        paid_at,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        CORR(discount, shipping_fee) OVER (PARTITION BY status) AS corr_discount_shipping_fee
    FROM kept_rows
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.store_name,
        p2.region
    FROM scored s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    region,
    metric_name,
    metric_value
FROM (
    SELECT
        region,
        discount,
        amount_share
    FROM joined
) src
UNPIVOT (
    metric_value FOR metric_name IN (discount AS DISCOUNT_VAL, amount_share AS AMOUNT_SHARE_VAL)
) u;
