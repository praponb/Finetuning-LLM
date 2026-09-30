WITH filtered AS (
    SELECT *, LENGTH(channel) AS channel_length
    FROM promotions
    WHERE starts_on > TO_DATE('2023-09-05')
      AND channel IS NOT NULL
      AND YEAR(starts_on) = 2023
),
matched_keys AS (
    SELECT channel
    FROM filtered
    EXCEPT ALL
    SELECT channel
    FROM promotions
    WHERE starts_on > '2023-11-01'
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.channel = k.channel
),
attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM reconciled e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on <= DATE_ADD(pr.ends_on, 3)
),
enriched AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_length,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel, promo_code), 4) AS discount_pct_share,
        COUNT(promotion_discount_pct) OVER w_hist AS channel_count_promotion_discount_pct,
        CUME_DIST() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct) AS discount_pct_cume_dist
    FROM attributed
    WINDOW w_hist AS (PARTITION BY channel)
),
with_windows AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_length,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        discount_pct_share,
        channel_count_promotion_discount_pct,
        discount_pct_cume_dist,
        MAX(channel_count_promotion_discount_pct) OVER (PARTITION BY promo_code) AS promo_code_max_channel_count_promotion_discount_pct,
        CUME_DIST() OVER (ORDER BY promo_id DESC NULLS FIRST) AS promo_id_cume_dist,
        PERCENT_RANK() OVER (ORDER BY promotion_discount_pct DESC NULLS FIRST) AS promotion_discount_pct_percent_rank,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct_cume_dist DESC) AS discount_pct_cume_dist_dense_rank
    FROM enriched
),
base AS (
    SELECT
        *,
        CONCAT_WS('-', channel, promo_code) AS channel_promo_code_key,
        SUBSTRING(channel, 1, 2) AS channel_prefix,
        COALESCE(promotion_promo_code, 'OTHER') AS promotion_promo_code_clean
    FROM with_windows
    WHERE ends_on > TO_DATE('2023-09-24')
      AND promotion_promo_code IS NOT NULL
),
base_promotions AS (
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        channel_length,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        discount_pct_share,
        discount_pct_cume_dist,
        promo_code_max_channel_count_promotion_discount_pct,
        promo_id_cume_dist,
        promotion_discount_pct_percent_rank,
        discount_pct_cume_dist_dense_rank,
        channel_prefix,
        promotion_promo_code_clean,
        discount_pct_share + channel_length AS discount_pct_share_plus_channel_length,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM base
    WHERE starts_on >= DATE '2023-04-14'
      AND channel IS NOT NULL
      AND discount_pct > 0.23
),
summary AS (
    SELECT
        promo_code,
        SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
        SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
        COUNT(*) AS event_count
    FROM base_promotions
    GROUP BY promo_code
)
SELECT
    promo_code,
    promo_code_list,
    num_promo_codes,
    event_count,
    CASE WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_A' WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'CORE' ELSE 'PRIORITY' END AS promo_code_zone
FROM summary;
