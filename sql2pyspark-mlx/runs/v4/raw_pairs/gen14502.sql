WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(discount_pct) OVER (PARTITION BY channel, promo_code) channel_max_discount_pct,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel,
        FIRST_VALUE(channel) IGNORE NULLS OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel_2
    FROM promotions
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
kept_keys AS (
    -- Set intersection on keys
    SELECT channel, promo_code
    FROM matched
    INTERSECT
    SELECT channel, promo_code
    FROM promotions
    WHERE promo_code <> 'WELCOME5'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    JOIN kept_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM verified a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(promo_code) clean_promo_code
    FROM matched_promotions
    WHERE starts_on > DATE '2023-09-22'
      AND promo_code = 'SPRING10'
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel, promo_code), 4) discount_pct_share,
        DENSE_RANK() OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id NULLS LAST) promo_id_dense_rank,
        ROUND(channel_max_discount_pct / SUM(channel_max_discount_pct) OVER (PARTITION BY promotion_promo_code_2), 4) channel_max_discount_pct_share
    FROM base
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT promo_code
    FROM enriched_promotions
    INTERSECT ALL
    SELECT promo_code
    FROM promotions
    WHERE ends_on BETWEEN '2023-10-01' AND '2024-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_promotions s
    JOIN kept_keys_2 k ON s.promo_code = k.promo_code
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    channel_max_discount_pct,
    first_known_channel,
    first_known_channel_2,
    CASE WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'OTHER_ZONE' WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'CORE' ELSE 'GROUP_A' END AS promo_code_zone
FROM kept_rows
ORDER BY promo_code_zone DESC
