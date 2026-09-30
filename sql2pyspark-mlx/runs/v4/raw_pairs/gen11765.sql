WITH latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id, customer_id ORDER BY order_date DESC) AS latest_rank
        FROM orders
    ) ranked
    WHERE latest_rank = 1
),
not_flagged AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
        )
),
with_window AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM not_flagged a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL 30 DAYS
),
filtered AS (
    SELECT
        *,
        LEAST(amount, discount_pct) AS least_amount_discount_pct,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        COALESCE(promo_code, 'UNKNOWN') AS resolved_promo_code
    FROM with_window
    WHERE store_id IS NOT NULL
)
SELECT
    CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    CASE WHEN days_since_order_date >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_order_date_tier,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    MIN(discount) AS min_discount,
    COUNT(*) FILTER (WHERE promo_code = 'VIP') AS vip_rows,
    GROUPING_ID() AS gid
FROM filtered
GROUP BY CUBE (CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN days_since_order_date >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING MIN(discount) < 0
ORDER BY vip_rows NULLS FIRST
