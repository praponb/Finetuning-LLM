WITH metrics AS (
    SELECT
        *,
        AVG(discount_pct) OVER (PARTITION BY promo_code, channel) AS promo_code_avg_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share_2
    FROM promotions
),
cleaned AS (
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        promo_code_avg_discount_pct,
        cumulative_discount_pct,
        discount_pct_avg_6_prior,
        discount_pct_share_2,
        COALESCE(discount_pct_share, 0) AS discount_pct_share_filled,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM metrics
    WHERE ends_on >= DATE '2023-06-11'
      AND channel IS NOT NULL
      AND discount_pct_share_2 BETWEEN 0.42 AND 0.56
),
enriched AS (
    SELECT
        *,
        LEAD(discount_pct, 1, 0) OVER (ORDER BY promo_code_avg_discount_pct, id) AS next_discount_pct,
        SUM(discount_pct_share_2) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_share_2
    FROM cleaned
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promo_code_avg_discount_pct, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
base AS (
    SELECT
        *,
        CEIL(cumulative_discount_pct_share_2) AS cumulative_discount_pct_share_2_whole,
        COALESCE(channel, 'NONE') AS channel_clean_2
    FROM ranked
    WHERE starts_on > DATE '2023-06-06'
)
SELECT
    id,
    channel,
    promo_code_avg_discount_pct,
    cumulative_discount_pct,
    discount_pct_avg_6_prior,
    discount_pct_share_2,
    discount_pct_share_filled,
    next_discount_pct,
    cumulative_discount_pct_share_2,
    CASE WHEN channel RLIKE '^(store|web)$' THEN 'GROUP_B' WHEN channel RLIKE '^(mobile|partner)$' THEN 'CORE' ELSE 'EXTENDED' END AS channel_zone
FROM base
ORDER BY channel_zone DESC, cumulative_discount_pct_share_2;
