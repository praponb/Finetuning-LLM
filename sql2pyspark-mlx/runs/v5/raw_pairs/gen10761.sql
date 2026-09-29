WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        FLOOR(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE starts_on >= DATE '2023-07-08'
      AND channel IS NOT NULL
),
matched AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 10)
),
deduped AS (
    SELECT *
    FROM matched
    QUALIFY RANK() OVER (PARTITION BY channel, promotion_promo_code ORDER BY discount_pct_whole DESC) = 1
),
metrics AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        discount_pct_whole,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        SUM(discount_pct_whole) OVER w_part AS last4_sum_discount_pct_whole,
        MAX(discount_pct_whole) OVER w_part AS last4_max_discount_pct_whole,
        AVG(discount_pct_whole) OVER w_part AS last4_avg_discount_pct_whole
    FROM deduped
    WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct ASC, id ASC, promo_id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, CASE WHEN promo_id >= 10 THEN 'High' WHEN promo_id >= 5 THEN 'Medium' ELSE 'Low' END AS promo_id_band
    FROM metrics
    WHERE channel = 'mobile'
),
base_promotions AS (
    SELECT
        *,
        COALESCE(discount_pct_whole, 0) AS discount_pct_whole_filled,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM filtered
    WHERE starts_on > DATE '2023-10-04'
      AND starts_on BETWEEN '2023-03-06' AND '2025-06-01'
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            channel,
            promo_code,
            last4_sum_discount_pct_whole
        FROM base_promotions
    ) src
    PIVOT (
        MAX(last4_sum_discount_pct_whole)
        FOR promo_code IN ('SPRING10' AS spring10, 'VIP' AS vip, 'BUNDLE' AS bundle)
    )
)
SELECT
    channel,
    spring10,
    bundle
FROM by_type
ORDER BY bundle ASC, spring10 ASC;
