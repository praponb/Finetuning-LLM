WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM promotions
    WHERE ends_on >= DATE '2023-09-11'
      AND channel IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on DESC, id) = 1
),
ranked_promotions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(discount_pct, 2) OVER (PARTITION BY channel ORDER BY rank_in_group, id) AS prev_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        SUM(discount_pct_whole) OVER (PARTITION BY channel ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_whole
    FROM ranked_promotions
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        running_discount_pct
    FROM enriched
) src
PIVOT (
    AVG(running_discount_pct) AS amount,
    COUNT(1) AS cnt
    FOR promo_code IN ('WELCOME5', 'VIP')
)
