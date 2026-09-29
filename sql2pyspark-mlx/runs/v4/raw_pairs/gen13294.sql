WITH joined AS (
    -- Attach orders attributes
    SELECT src.*, b.channel
    FROM shipments src
    INNER JOIN orders b
        ON src.order_id = b.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM deduped s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.shipped_date >= pr.starts_on
       AND s.shipped_date < pr.ends_on + INTERVAL '14' DAY
),
with_window_shipments AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_window s
    INNER JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.shipped_date BETWEEN pr.starts_on AND pr.ends_on
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN promo_id_2 >= 11 THEN 'HIGH' ELSE 'LOW' END AS promo_id_2_tier,
    PERCENTILE_APPROX(promotion_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS promotion_discount_pct_quartiles,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    ROUND(SUM(promotion_discount_pct) / NULLIF(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END), 0), 2) AS promotion_discount_pct_per_delivered,
    SUM(promotion_discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_total_promotion_discount_pct
FROM with_window_shipments
GROUP BY CUBE (CASE WHEN promo_id_2 >= 11 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY spring10_total_promotion_discount_pct NULLS LAST
