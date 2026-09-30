WITH attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL '7' DAY
),
kept_keys AS (
    SELECT promo_code, channel
    FROM attributed
    EXCEPT DISTINCT
    SELECT promo_code, channel
    FROM promotions
    WHERE MONTH(ends_on) IN (2, 9, 11)
),
kept_rows AS (
    SELECT s.*
    FROM attributed s
    INNER JOIN kept_keys k ON s.promo_code = k.promo_code AND s.channel = k.channel
),
matched AS (
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM kept_rows e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on <= DATE_ADD(pr.ends_on, 3)
),
with_windows AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        promotion_discount_pct_2,
        STDDEV_SAMP(promo_id) OVER w_hist AS channel_stddev_samp_promo_id,
        PERCENT_RANK() OVER (PARTITION BY channel, promotion_promo_code_2 ORDER BY promo_id_2 DESC NULLS FIRST) AS promo_id_2_percent_rank,
        RANK() OVER (PARTITION BY promotion_promo_code_2 ORDER BY promotion_discount_pct_2 DESC NULLS LAST) AS promotion_discount_pct_2_rank
    FROM matched
    WINDOW w_hist AS (PARTITION BY channel)
),
with_windows_promotions AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY promotion_discount_pct_2) AS promotion_discount_pct_2_dense_rank,
        ROUND(promotion_discount_pct_2_rank / SUM(promotion_discount_pct_2_rank) OVER (PARTITION BY promotion_promo_code), 4) AS promotion_discount_pct_2_rank_share,
        RANK() OVER (ORDER BY promo_id DESC NULLS LAST) AS promo_id_rank
    FROM with_windows
),
kept_keys_2 AS (
    SELECT channel, id
    FROM with_windows_promotions
    EXCEPT
    SELECT channel, id
    FROM promotions
    WHERE promo_code IN ('WELCOME5', 'BUNDLE')
),
verified AS (
    SELECT s.*
    FROM with_windows_promotions s
    INNER JOIN kept_keys_2 k ON s.channel = k.channel AND s.id = k.id
),
totals AS (
    SELECT
        CASE WHEN promotion_discount_pct_2_rank_share >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS promotion_discount_pct_2_rank_share_tier,
        DATE_TRUNC('YEAR', starts_on) AS year_start,
        ROUND(AVG(promo_id_2_percent_rank), 2) AS avg_promo_id_2_percent_rank,
        SIZE(COLLECT_SET(promotion_promo_code_2)) AS num_promotion_promo_code_2s
    FROM verified
    GROUP BY CASE WHEN promotion_discount_pct_2_rank_share >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', starts_on)
)
SELECT
    promotion_discount_pct_2_rank_share_tier,
    year_start,
    avg_promo_id_2_percent_rank,
    num_promotion_promo_code_2s
FROM totals
ORDER BY year_start DESC NULLS LAST
