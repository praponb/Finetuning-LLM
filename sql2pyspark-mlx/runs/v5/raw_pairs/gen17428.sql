WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions a
    JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL '30' DAY
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        channel,
        promo_id,
        promotion_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id DESC NULLS LAST, id, promo_id) AS latest_rank
        FROM with_window
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_id,
        MIN(promotion_discount_pct) OVER (PARTITION BY channel) channel_min_promotion_discount_pct,
        SUM(promotion_discount_pct) OVER (ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_promotion_discount_pct
    FROM ranked
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(channel_min_promotion_discount_pct, running_promotion_discount_pct) greatest_channel_min_promotion_discount_pct_running_promotion_discount_pct,
        FLOOR(promo_id) promo_id_whole
    FROM with_windows
    WHERE promo_id < 5
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(running_promotion_discount_pct, TRUE) OVER (ORDER BY channel_min_promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_running_promotion_discount_pct,
        RANK() OVER (ORDER BY channel_min_promotion_discount_pct) channel_min_promotion_discount_pct_rank,
        NTILE(10) OVER (ORDER BY promo_id_whole DESC, id, promo_id) promo_id_whole_bucket
    FROM cleaned
)
SELECT
    id,
    promo_id,
    channel_min_promotion_discount_pct,
    greatest_channel_min_promotion_discount_pct_running_promotion_discount_pct,
    channel_min_promotion_discount_pct_rank,
    promo_id_whole_bucket
FROM enriched
ORDER BY channel_min_promotion_discount_pct DESC NULLS FIRST;
