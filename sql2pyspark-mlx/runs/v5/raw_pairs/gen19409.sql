WITH with_windows AS (
    SELECT
        *,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        ROW_NUMBER() OVER (ORDER BY starts_on DESC, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel, promo_code), 4) AS discount_pct_share
    FROM promotions
    WHERE promo_code != 'BUNDLE'
)
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    CASE WHEN discount_pct_share >= 0.67 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_share_tier,
    DATE_TRUNC('YEAR', starts_on) AS year_start,
    COUNT(DISTINCT channel) AS distinct_channels,
    COUNT_IF(promo_code = 'SPRING10') AS spring10_events,
    COUNT(*) AS cnt,
    COUNT(DISTINCT channel) AS distinct_channels_2,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY CUBE (DATE_TRUNC('MONTH', ends_on), CASE WHEN discount_pct_share >= 0.67 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', starts_on))
