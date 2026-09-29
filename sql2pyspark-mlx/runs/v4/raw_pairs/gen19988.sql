WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code, channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        MAX(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_max_discount_pct
    FROM promotions
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    discount_pct_share,
    CASE WHEN channel RLIKE '^(mobile|web)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS channel_zone
FROM metrics
ORDER BY discount_pct_share ASC;
