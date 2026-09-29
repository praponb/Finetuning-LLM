WITH filtered AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(ends_on, 'MMM yyyy') AS ends_on_label,
        LOWER(channel) AS channel_lower,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-09-14')
      AND channel IS NOT NULL
),
enriched AS (
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        ends_on_label,
        channel_lower,
        MIN(discount_pct) OVER w_ordered AS last4_min_discount_pct,
        MAX(discount_pct) OVER w_ordered AS last4_max_discount_pct,
        AVG(discount_pct) OVER w_ordered AS last4_avg_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS event_seq
    FROM filtered
    WHERE discount_pct > 0.32
    WINDOW w_ordered AS (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT
        id,
        promo_code,
        discount_pct,
        ends_on_label,
        channel_lower,
        last4_max_discount_pct,
        last4_avg_discount_pct,
        event_seq
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY last4_avg_discount_pct DESC, id) = 1
),
deduped AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY last4_avg_discount_pct DESC, id) = 1
),
grouped AS (
    SELECT
        CASE WHEN last4_max_discount_pct >= 0.33 THEN 'LARGE' ELSE 'SMALL' END AS last4_max_discount_pct_tier,
        COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END) AS welcome5_count,
        SUM(CASE WHEN promo_code = 'SPRING10' THEN last4_max_discount_pct ELSE 0 END) AS spring10_last4_max_discount_pct,
        AVG(last4_max_discount_pct) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_avg_last4_max_discount_pct,
        ROUND(SUM(last4_max_discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS last4_max_discount_pct_per_spring10
    FROM deduped
    GROUP BY 1
)
SELECT
    last4_max_discount_pct_tier,
    welcome5_count,
    spring10_last4_max_discount_pct,
    last4_max_discount_pct_per_spring10
FROM grouped
ORDER BY spring10_last4_max_discount_pct DESC;
