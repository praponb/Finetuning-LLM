WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN promo_code IN ('BUNDLE', 'VIP') THEN 'Priority' ELSE 'Other' END AS promo_code_group
    FROM promotions
    WHERE starts_on >= '2023-03-03'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_group,
        ROUND(discount_pct, 1) AS discount_pct_rounded,
        ROUND(discount_pct, 1) AS discount_pct_rounded_2,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM base
    WHERE starts_on >= DATE '2023-07-19'
      AND channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(discount_pct_rounded) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS prev_discount_pct_rounded,
        LEAD(discount_pct_rounded_2) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS next_discount_pct_rounded_2,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) AS row_seq,
        LAG(discount_pct_rounded_2, 1) OVER (PARTITION BY channel ORDER BY discount_pct_rounded, id) AS prev_discount_pct_rounded_2
    FROM filtered
    WHERE channel IN ('partner', 'web', 'store')
),
with_windows_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        promo_code_group,
        discount_pct_rounded_2,
        prev_discount_pct_rounded,
        next_discount_pct_rounded_2,
        row_seq,
        prev_discount_pct_rounded_2,
        RANK() OVER (PARTITION BY channel, promo_code ORDER BY next_discount_pct_rounded_2) AS next_discount_pct_rounded_2_rank,
        NTILE(4) OVER (ORDER BY discount_pct_rounded_2, id) AS discount_pct_rounded_2_bucket,
        LAST_VALUE(row_seq, TRUE) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_rounded_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq
    FROM with_windows
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    next_discount_pct_rounded_2,
    discount_pct_rounded_2_bucket,
    last_known_row_seq
FROM with_windows_promotions
ORDER BY ends_on;
