WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, HOUR(paid_at) AS paid_at_hour
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-07-25 02:00:00')
      AND method IS NOT DISTINCT FROM 'card'
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.amount,
        a.paid_at,
        a.status,
        a.paid_at_hour,
        d.discount
    FROM base a
    INNER JOIN orders d
        ON a.order_id = d.id
    WHERE d.shipping_fee >= 23
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.channel,
        p.amount AS order_amount
    FROM joined a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, order_id
    FROM enriched
    INTERSECT
    SELECT id, order_id
    FROM payments
    WHERE status != 'FAILED'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.order_id = k.order_id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, method
    FROM verified
    EXCEPT DISTINCT
    SELECT status, method
    FROM payments
    WHERE paid_at >= '2024-07-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.method = k.method
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.amount,
        a.paid_at_hour,
        a.discount,
        a.channel,
        a.order_amount,
        p.shipping_fee
    FROM reconciled a
    INNER JOIN orders p
        ON a.order_id = p.id
),
enriched_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        discount,
        channel,
        order_amount,
        shipping_fee,
        MAX(order_amount) OVER (PARTITION BY order_id) AS order_id_max_order_amount,
        CUME_DIST() OVER (ORDER BY order_amount DESC NULLS LAST) AS order_amount_cume_dist,
        COUNT(amount) OVER w_recent AS method_count_amount
    FROM enriched_payments
    WINDOW w_recent AS (PARTITION BY method, channel)
)
-- Ranking and gap features
SELECT
    order_amount,
    method_count_amount,
    RANK() OVER w_hist AS order_amount_rank
FROM enriched_payments_2
WINDOW w_hist AS (ORDER BY order_amount DESC NULLS FIRST);
