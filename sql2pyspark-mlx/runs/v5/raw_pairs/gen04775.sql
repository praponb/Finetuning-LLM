WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, MONTH(ends_on) AS ends_on_month
    FROM promotions
    WHERE channel IS DISTINCT FROM 'mobile'
)
-- Running and moving aggregates
SELECT
    id,
    channel,
    starts_on,
    CUME_DIST() OVER (PARTITION BY channel, promo_code ORDER BY ends_on_month NULLS LAST) AS ends_on_month_cume_dist,
    AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
    PERCENT_RANK() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC) AS discount_pct_percent_rank
FROM base;
