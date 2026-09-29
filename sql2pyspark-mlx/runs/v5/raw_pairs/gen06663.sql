WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        INITCAP(TRIM(channel)) AS channel_title
    FROM promotions
    WHERE starts_on > DATE '2023-09-11'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY channel, promo_code ORDER BY rank_in_group) AS rank_in_group_2
    FROM ranked
    QUALIFY rank_in_group_2 = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        rank_in_group,
        rank_in_group_2,
        LEAD(discount_pct, 1) OVER (PARTITION BY promo_code, channel ORDER BY rank_in_group, id) AS next_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
    FROM deduped
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(rank_in_group, 1) AS rank_in_group_rounded
    FROM with_windows
    WHERE rank_in_group_2 > 1
),
cleaned_promotions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        rank_in_group,
        rank_in_group_2,
        next_discount_pct,
        rank_in_group_rounded,
        rank_in_group_rounded * next_discount_pct AS rank_in_group_rounded_times_next_discount_pct,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM cleaned
    WHERE channel IS NOT NULL
      AND next_discount_pct BETWEEN 0.18 AND 0.22
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY rank_in_group DESC, id) AS rn
    FROM cleaned_promotions
    QUALIFY rn = 1
)
SELECT
    id,
    rank_in_group,
    rank_in_group_2,
    next_discount_pct,
    rank_in_group_rounded,
    channel_clean,
    rn
FROM deduped_promotions
ORDER BY rank_in_group NULLS LAST;
