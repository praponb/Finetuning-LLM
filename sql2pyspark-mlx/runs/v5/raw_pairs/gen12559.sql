WITH base AS (
    SELECT
        *,
        SUBSTRING(channel, 1, 2) AS channel_prefix,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE discount_pct >= 0.2
)
SELECT
    channel,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    APPROX_PERCENTILE(discount_pct, 0.5) AS discount_pct_p50
FROM base
GROUP BY channel
HAVING APPROX_PERCENTILE(discount_pct, 0.5) > 0
