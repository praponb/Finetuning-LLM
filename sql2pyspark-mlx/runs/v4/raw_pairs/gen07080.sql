WITH cleaned AS (
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
        ROUND(discount, 0) AS discount_rounded
    FROM orders
    WHERE channel <> 'partner'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS dedup_rank
        FROM cleaned
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(amount, 1) OVER (ORDER BY discount_rounded, id) AS prev_amount,
        FIRST_VALUE(discount_rounded) IGNORE NULLS OVER (ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_rounded
    FROM ranked
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.payment_status,
        x.payment_paid_at
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status, c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.channel,
    s.discount,
    s.discount_rounded,
    s.dedup_rank,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS channel_zone,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM with_latest s
ORDER BY dedup_rank NULLS FIRST
