WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        ROUND(amount * 0.9, 2) AS adjusted_amount,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE order_date >= DATE '2023-06-12'
      AND channel IS DISTINCT FROM 'partner'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.order_id IN (16, 25, 27, 30)
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(adjusted_amount) OVER (PARTITION BY store_id ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS adjusted_amount_avg_3_prior,
        LAG(discount, 1) OVER (PARTITION BY channel ORDER BY order_date ASC, id ASC) AS prev_discount
    FROM eligible
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_windows e
    JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date <= DATE_ADD(pr.ends_on, 7)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        order_date,
        discount,
        shipping_fee,
        adjusted_amount,
        promo_id,
        promo_code,
        AVG(promo_id) OVER w_running AS promo_id_avg_6,
        AVG(adjusted_amount) OVER w_grp AS adjusted_amount_avg_3_prior_2
    FROM attributed
    WINDOW w_running AS (PARTITION BY store_id ORDER BY promo_id ASC, id ASC, promo_id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY promo_code ORDER BY prev_discount ASC, id ASC, promo_id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    MIN(discount) AS min_discount,
    PERCENTILE_APPROX(shipping_fee, 0.5) AS shipping_fee_p50,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    SUM(promo_id_avg_6) AS total_promo_id_avg_6,
    MAX(promo_id) FILTER (WHERE promo_code = 'VIP') AS vip_max_promo_id,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM enriched
GROUP BY activity_month
ORDER BY min_discount DESC NULLS FIRST;
