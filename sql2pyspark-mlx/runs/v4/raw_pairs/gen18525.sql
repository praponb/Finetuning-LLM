WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount_pct, 0) discount_pct_rounded,
        CASE WHEN promo_code = 'SPRING10' THEN 1 ELSE 0 END is_spring10,
        COALESCE(channel, 'N/A') resolved_channel
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-09-29')
      AND channel IS NOT NULL
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
        is_spring10,
        resolved_channel,
        AVG(is_spring10) OVER (ORDER BY is_spring10, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) is_spring10_avg_2_prior,
        NTILE(5) OVER (ORDER BY discount_pct_rounded DESC, id) discount_pct_rounded_bucket,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) discount_pct_share,
        FIRST_VALUE(discount_pct_rounded) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_discount_pct_rounded
    FROM base
),
base_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_rounded,
        resolved_channel,
        is_spring10_avg_2_prior,
        first_known_discount_pct_rounded,
        CAST(discount_pct AS INT) discount_pct_int,
        first_known_discount_pct_rounded - discount_pct_rounded_bucket first_known_discount_pct_rounded_minus_discount_pct_rounded_bucket,
        COALESCE(channel, 'OTHER') channel_filled
    FROM with_windows
    WHERE starts_on >= DATE '2024-03-06'
      AND channel IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS row_num
    FROM base_promotions
    QUALIFY row_num = 1
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_rounded,
    is_spring10_avg_2_prior,
    discount_pct_int,
    first_known_discount_pct_rounded_minus_discount_pct_rounded_bucket,
    channel_filled
FROM deduped
ORDER BY discount_pct_int;
