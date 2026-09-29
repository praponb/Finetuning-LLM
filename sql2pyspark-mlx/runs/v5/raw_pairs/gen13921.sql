WITH with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM orders src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date < sub.end_date + INTERVAL 30 DAYS
),
joined AS (
    SELECT
        e.id,
        e.customer_id,
        e.store_id,
        e.order_date,
        e.amount,
        e.subscription_id,
        d.store_name
    FROM with_window e
    LEFT JOIN stores d
        ON e.store_id <=> d.id
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY subscription_id) subscription_id_rank,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY amount, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount,
        SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount
    FROM joined
),
latest AS (
    SELECT
        id,
        customer_id,
        amount,
        subscription_id,
        subscription_id_rank,
        running_amount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, customer_id ORDER BY order_date, id, subscription_id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
latest_match AS (
    SELECT
        s.id,
        s.amount,
        s.subscription_id,
        s.running_amount,
        latest.paid_at
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    SELECT s.*, top1.quantity
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
remaining_keys AS (
    SELECT id
    FROM with_last
    INTERSECT ALL
    SELECT id
    FROM orders
    WHERE shipping_fee <= 29
),
verified AS (
    SELECT s.*
    FROM with_last s
    INNER JOIN remaining_keys k ON s.id = k.id
),
totals AS (
    SELECT
        DATE_TRUNC('MONTH', paid_at) AS activity_month,
        CASE WHEN running_amount >= 7230 THEN 'LARGE' ELSE 'SMALL' END AS running_amount_tier,
        MIN(paid_at) AS first_paid_at,
        COUNT(DISTINCT id) AS distinct_ids,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes
    FROM verified
    GROUP BY DATE_TRUNC('MONTH', paid_at), CASE WHEN running_amount >= 7230 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    activity_month,
    running_amount_tier,
    first_paid_at,
    distinct_ids,
    span_minutes
FROM totals
ORDER BY distinct_ids NULLS LAST;
