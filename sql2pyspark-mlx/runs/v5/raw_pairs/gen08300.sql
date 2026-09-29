WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        DATE_TRUNC('WEEK', ends_on) ends_on_period,
        COALESCE(channel, 'UNKNOWN') channel_filled
    FROM promotions
    WHERE ends_on > '2023-11-01'
)
-- Ranking and gap features
SELECT
    id,
    channel_filled,
    LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_channel,
    DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct) discount_pct_dense_rank,
    AVG(discount_pct) OVER (ORDER BY ends_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_pct_avg_2,
    NTILE(3) OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) discount_pct_bucket,
    CUME_DIST() OVER (ORDER BY discount_pct) discount_pct_cume_dist
FROM base
