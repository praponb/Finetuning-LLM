WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER w_part AS discount_pct_bucket,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_pct_avg_5,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        NTILE(3) OVER w_part AS discount_pct_bucket_2,
        CUME_DIST() OVER w_grp AS discount_pct_cume_dist
    FROM promotions
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY discount_pct DESC, id ASC), w_grp AS (ORDER BY discount_pct DESC)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(discount_pct_bucket, 0) AS discount_pct_bucket_rounded,
        ADD_MONTHS(starts_on, 3) AS starts_on_plus_months,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM with_windows
    WHERE promo_code IN ('WELCOME5', 'SPRING10', 'VIP')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'DIRECT') AS channel_filled_2
    FROM valid
    WHERE ends_on > TO_DATE('2023-08-28')
      AND channel IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL 14 DAYS
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    promo_id,
    channel,
    kpi,
    val
FROM (
    SELECT
        id,
        promo_id,
        channel,
        discount_pct,
        discount_pct_bucket_rounded,
        promotion_discount_pct
    FROM matched
) src
UNPIVOT (
    val FOR kpi IN (discount_pct, discount_pct_bucket_rounded AS DISCOUNT_PCT_BUCKET_ROUNDED, promotion_discount_pct AS PROMOTION_DISCOUNT_PCT_VAL)
)
