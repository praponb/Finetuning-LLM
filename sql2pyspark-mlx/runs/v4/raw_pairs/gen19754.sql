WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS rank_in_group
        FROM promotions
    ) x
    WHERE rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS discount_pct_per_mobile,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev
FROM latest
GROUP BY activity_month;
