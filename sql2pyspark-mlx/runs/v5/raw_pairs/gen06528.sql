WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        COALESCE(discount, 0) AS discount_filled
    FROM orders
    WHERE order_date >= DATE '2023-05-16'
      AND channel IS NOT NULL
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.payment_amount,
        latest.payment_status
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount, c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, channel
    FROM latest_match
    EXCEPT DISTINCT
    SELECT status, channel
    FROM orders
    WHERE status IN ('RETURNED', 'CANCELLED', 'SHIPPED')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.channel = k.channel
),
valid_orders AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        discount_filled,
        payment_amount,
        payment_status,
        LOWER(status) AS status_lower
    FROM reconciled
    WHERE order_date > DATE '2023-06-15'
      AND channel IN ('mobile', 'web')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.amount <= 1350
        )
)
SELECT
    s.customer_id,
    s.store_id,
    s.order_date,
    s.shipping_fee,
    s.payment_amount,
    s.payment_status,
    s.status_lower,
    CASE WHEN payment_status RLIKE '^(SUCCESS|FAILED)$' THEN 'GROUP_A' WHEN payment_status RLIKE '^(REFUNDED)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS payment_status_zone,
    (SELECT MIN(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS min_store_sale_units
FROM not_flagged s;
