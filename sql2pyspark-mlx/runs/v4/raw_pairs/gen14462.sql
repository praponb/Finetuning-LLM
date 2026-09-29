WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        CAST(discount_pct AS INT) AS discount_pct_int,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM promotions
    WHERE starts_on >= DATE '2024-01-30'
      AND MONTH(ends_on) IN (2, 5, 9)
)
SELECT
    *,
    COUNT(discount_pct_int) OVER (PARTITION BY channel) AS channel_count_discount_pct_int,
    SUM(discount_pct_int) OVER w_running AS cumulative_discount_pct_int,
    AVG(discount_pct) OVER w_ordered AS discount_pct_avg_2_prior,
    NTILE(4) OVER (PARTITION BY channel ORDER BY discount_pct_int DESC, id) AS discount_pct_int_bucket
FROM base
WINDOW w_running AS (ORDER BY discount_pct_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY promo_code, channel ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING);
