WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.order_date
        )
),
not_flagged_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date < '2024-10-20'
        )
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM not_flagged_orders e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date < pr.ends_on + INTERVAL '30' DAY
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN channel = 'partner' THEN 1 ELSE 0 END AS is_partner,
        COALESCE(promo_code, 'OTHER') AS resolved_promo_code
    FROM with_window
    WHERE discount BETWEEN 0.15 AND 0.34
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY is_partner DESC, id, promo_id) AS is_partner_bucket,
        LEAD(discount, 1, 0) OVER (PARTITION BY promo_code ORDER BY amount, id, promo_id) AS next_discount
    FROM filtered
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(discount) OVER (ORDER BY shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        FIRST_VALUE(discount_pct, TRUE) OVER (ORDER BY shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct,
        SUM(shipping_fee) OVER (PARTITION BY promo_code, channel ORDER BY is_partner, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipping_fee
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        promo_id,
        store_id,
        discount_pct,
        next_discount
    FROM metrics
) src
UNPIVOT (
    metric_value FOR metric IN (discount_pct, next_discount)
);
