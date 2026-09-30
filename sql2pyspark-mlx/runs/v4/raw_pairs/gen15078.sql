WITH filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        LENGTH(channel) AS channel_length,
        CONCAT_WS(',', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
)
SELECT
    *,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
    AVG(channel_length) OVER (ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS channel_length_avg_2_prior
FROM filtered;
