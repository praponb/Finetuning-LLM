WITH cleaned AS (
    SELECT
        *,
        DAY(starts_on) AS starts_on_day,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
)
SELECT
    id,
    starts_on,
    ends_on,
    discount_pct,
    resolved_channel,
    FIRST_VALUE(channel, TRUE) OVER w_running AS first_known_channel,
    FIRST_VALUE(starts_on_day, TRUE) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_starts_on_day,
    CUME_DIST() OVER (PARTITION BY promo_code, channel ORDER BY starts_on_day NULLS FIRST) AS starts_on_day_cume_dist,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
    NTILE(10) OVER (PARTITION BY promo_code ORDER BY starts_on_day DESC, id) AS starts_on_day_bucket
FROM cleaned
WINDOW w_running AS (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
