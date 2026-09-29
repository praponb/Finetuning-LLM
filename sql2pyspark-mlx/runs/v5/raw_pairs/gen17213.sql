WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        ROUND(discount_pct * 0.9, 2) AS adjusted_discount_pct
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-09-12')
      AND channel IS NOT NULL
),
filtered_promotions AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN channel IN ('web', 'mobile') THEN 'Group A' ELSE 'Other' END AS channel_group
    FROM filtered
    WHERE starts_on >= DATE '2023-09-11'
      AND ends_on <= '2025-07-08'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered_promotions s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        adjusted_discount_pct,
        promo_id,
        promotion_discount_pct,
        MIN(promo_id) OVER w_grp AS last7_min_promo_id,
        AVG(promo_id) OVER w_grp AS last7_avg_promo_id,
        MAX(promo_id) OVER w_grp AS last7_max_promo_id,
        SUM(promo_id) OVER w_grp AS last7_sum_promo_id
    FROM with_window
    WINDOW w_grp AS (PARTITION BY channel ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM metrics s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on <= DATE_ADD(pr.ends_on, 3)
)
SELECT
    id,
    channel,
    promo_code,
    promo_id,
    last7_min_promo_id,
    last7_avg_promo_id,
    last7_max_promo_id,
    last7_sum_promo_id,
    promo_id_2,
    CASE WHEN channel RLIKE '^(partner|web)$' THEN 'CORE' WHEN channel RLIKE '^(mobile|store)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS channel_zone
FROM matched
ORDER BY last7_max_promo_id DESC NULLS FIRST;
