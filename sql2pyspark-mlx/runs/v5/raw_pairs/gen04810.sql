WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        COALESCE(amount, 0) AS amount_filled,
        CONCAT_WS(',', status, method) AS status_method_key,
        COALESCE(method, 'NONE') AS method_clean
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-06-12 00:00:00'
      AND method IS DISTINCT FROM 'cash'
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, status
    FROM cleaned
    EXCEPT ALL
    SELECT order_id, status
    FROM payments
    WHERE amount <= 790
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
),
with_dims AS (
    -- Attach orders attributes
    SELECT e.*, b.channel
    FROM reconciled e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.order_date,
        b.status AS order_status
    FROM with_dims a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
joined_payments AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.amount,
        src.amount_filled,
        src.status_method_key,
        src.method_clean,
        src.channel,
        src.order_date,
        src.order_status,
        p.shipping_fee
    FROM joined src
    INNER JOIN orders p
        ON src.order_id = p.id
    WHERE p.store_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM joined_payments
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM payments
    WHERE paid_at < '2024-04-11'
),
reconciled_payments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined_payments s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.order_id = k.order_id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_id,
            channel,
            shipping_fee
        FROM reconciled_payments
    ) src
    PIVOT (
        MAX(shipping_fee)
        FOR channel IN ('mobile', 'partner', 'web')
    )
)
SELECT
    s.order_id,
    s.mobile,
    s.partner,
    s.web,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS sum_order_item_quantity
FROM pivoted s
ORDER BY order_id DESC NULLS FIRST, mobile
LIMIT 10;
