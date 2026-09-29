WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(discount_pct, 1) AS discount_pct_rounded
    FROM promotions
    WHERE discount_pct <= 0.18
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_rounded,
        AVG(discount_pct_rounded) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_rounded_avg_6_prior,
        NTILE(10) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM valid
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL '14' DAY
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_rounded,
        discount_pct_rounded_avg_6_prior,
        discount_pct_bucket,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        LAG(discount_pct_bucket, 1) OVER w_recent AS prev_discount_pct_bucket,
        NTILE(5) OVER w_recent_2 AS discount_pct_rounded_avg_6_prior_bucket,
        LAG(discount_pct_rounded, 1, 0) OVER w_hist AS prev_discount_pct_rounded,
        LEAD(discount_pct_rounded, 1) OVER w_ordered AS next_discount_pct_rounded
    FROM matched
    WINDOW w_recent AS (ORDER BY promo_id, id, promo_id), w_recent_2 AS (PARTITION BY promotion_promo_code, promo_code ORDER BY discount_pct_rounded_avg_6_prior DESC, id, promo_id), w_hist AS (PARTITION BY promo_code ORDER BY promo_id, id, promo_id), w_ordered AS (PARTITION BY promotion_promo_code ORDER BY promo_id, id, promo_id)
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN discount_pct >= 0.3 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_tier,
        SIZE(COLLECT_SET(channel)) AS num_channels,
        AVG(prev_discount_pct_rounded) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_avg_prev_discount_pct_rounded,
        COUNT(*) AS cnt
    FROM with_windows
    GROUP BY 1
)
SELECT discount_pct_tier, num_channels
FROM summary
ORDER BY discount_pct_tier, num_channels;
