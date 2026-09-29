WITH matched_keys AS (
    SELECT channel, promo_code
    FROM promotions
    EXCEPT ALL
    SELECT channel, promo_code
    FROM promotions
    WHERE starts_on > '2024-01-26'
),
reconciled AS (
    SELECT s.*
    FROM promotions s
    JOIN matched_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
)
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    MIN(discount_pct) AS min_discount_pct,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS discount_pct_per_partner,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS discount_pct_per_mobile,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END), 0), 2) AS discount_pct_per_welcome5
FROM reconciled
GROUP BY DATE_TRUNC('MONTH', ends_on)
HAVING discount_pct_per_mobile >= 0;
