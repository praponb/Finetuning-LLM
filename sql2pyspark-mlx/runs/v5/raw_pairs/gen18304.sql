WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(discount_pct AS INT) AS discount_pct_int,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
      AND discount_pct >= 0.24
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on DESC, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on <= DATE_ADD(pr.ends_on, 3)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(promotion_discount_pct, 0) AS promotion_discount_pct_filled,
        COALESCE(promotion_promo_code_2, 'UNKNOWN') AS resolved_promotion_promo_code_2
    FROM matched
    WHERE promo_id != 8
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    discount_pct_int,
    channel_clean,
    promo_id,
    promotion_promo_code,
    promo_id_2
FROM valid
ORDER BY promotion_promo_code DESC NULLS LAST
