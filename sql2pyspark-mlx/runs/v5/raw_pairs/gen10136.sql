WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders a
    JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.order_date BETWEEN pr.starts_on AND pr.ends_on
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id, promo_id) AS dedup_rank
    FROM with_window
    QUALIFY dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.unit_price BETWEEN 150 AND 210
        )
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.method,
        latest.payment_status
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.method, c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id = 36
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        order_date,
        discount,
        shipping_fee,
        promo_id,
        dedup_rank,
        method,
        payment_status,
        AVG(discount_pct) OVER (ORDER BY discount, id, promo_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior,
        FIRST_VALUE(discount) IGNORE NULLS OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount
    FROM not_flagged_orders
)
SELECT
    id,
    store_id,
    discount,
    shipping_fee,
    promo_id,
    discount_pct_avg_2_prior,
    first_known_discount
FROM with_windows
ORDER BY discount DESC NULLS FIRST
