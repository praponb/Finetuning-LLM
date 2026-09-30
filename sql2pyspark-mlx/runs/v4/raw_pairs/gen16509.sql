WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        CONCAT_WS(',', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM promotions
    WHERE ends_on > DATE '2023-06-30'
      AND channel IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned e
    JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched src
    JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM with_window src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
grouped AS (
    -- Grouped metrics
    SELECT
        channel,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        COUNT_IF(channel = 'mobile') AS mobile_events,
        ROUND(AVG(promotion_discount_pct_2), 2) AS avg_promotion_discount_pct_2,
        COUNT(*) AS row_count,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM matched_promotions
    GROUP BY channel
)
SELECT
    channel,
    channel_list,
    mobile_events,
    avg_promotion_discount_pct_2,
    row_count,
    CASE WHEN channel RLIKE '^(web|store)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS channel_zone
FROM grouped
ORDER BY avg_promotion_discount_pct_2 NULLS LAST, mobile_events;
