WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY ends_on DESC, id) AS seq_no,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        MIN(discount_pct) OVER (PARTITION BY channel) AS channel_min_discount_pct
    FROM promotions
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        seq_no,
        channel_min_discount_pct,
        ROUND(discount_pct, 0) AS discount_pct_rounded,
        DATE_FORMAT(ends_on, 'yyyy-MM-dd') AS ends_on_label
    FROM enriched
    WHERE channel IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, channel
    FROM base
    EXCEPT
    SELECT id, channel
    FROM promotions
    WHERE promo_code = 'SPRING10'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id AND s.channel = k.channel
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM reconciled a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY promo_id DESC NULLS FIRST) AS promo_id_rank,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY promotion_promo_code_2) AS promotion_promo_code_2_stddev_samp_seq_no,
        CUME_DIST() OVER (ORDER BY discount_pct_rounded DESC NULLS FIRST) AS discount_pct_rounded_cume_dist,
        ROUND(promotion_discount_pct / SUM(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code_2), 4) AS promotion_discount_pct_share
    FROM matched
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM metrics src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on <= DATE_ADD(pr.ends_on, 10)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        promo_code,
        promotion_discount_pct,
        promotion_discount_pct_2,
        discount_pct_rounded,
        promotion_discount_pct_share
    FROM with_window_promotions
) src
UNPIVOT (
    value FOR metric_name IN (promotion_discount_pct AS PROMOTION_DISCOUNT_PCT, promotion_discount_pct_2, discount_pct_rounded, promotion_discount_pct_share AS PROMOTION_DISCOUNT_PCT_SHARE_VAL)
)
