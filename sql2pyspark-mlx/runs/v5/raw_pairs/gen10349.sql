WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        LAST_DAY(ends_on) AS ends_on_month_end,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on_month_end BETWEEN pr.starts_on AND pr.ends_on
),
attributed_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM attributed s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on_month_end >= pr.starts_on
       AND s.ends_on_month_end <= DATE_ADD(pr.ends_on, 10)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM attributed_promotions
    QUALIFY RANK() OVER (PARTITION BY promotion_promo_code, channel ORDER BY promotion_discount_pct ASC) = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM ranked a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on <= DATE_ADD(pr.ends_on, 7)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', ends_on_month_end) AS activity_month,
    CASE WHEN promotion_discount_pct_2 >= 0.32 THEN 'PREMIUM' ELSE 'STANDARD' END AS promotion_discount_pct_2_tier,
    COUNT(*) AS row_count,
    ROUND(SUM(promo_id_3) / NULLIF(COUNT(CASE WHEN promotion_promo_code_2 = 'WELCOME5' THEN 1 END), 0), 2) AS promo_id_3_per_welcome5
FROM with_window
GROUP BY activity_month, promotion_discount_pct_2_tier;
