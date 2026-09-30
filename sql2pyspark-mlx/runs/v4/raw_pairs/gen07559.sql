WITH valid AS (
    SELECT
        *,
        DATE_ADD(starts_on, 69) AS starts_on_plus_69d,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
)
SELECT
    *,
    AVG(discount_pct) OVER w_running AS discount_pct_avg_2,
    AVG(discount_pct) OVER (ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_avg_6
FROM valid
WINDOW w_running AS (ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
