WITH remaining_keys AS (
    -- Set difference on keys
    SELECT channel
    FROM promotions
    EXCEPT
    SELECT channel
    FROM promotions
    WHERE discount_pct > 0.14
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN remaining_keys k ON s.channel = k.channel
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM verified a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(promotion_discount_pct) OVER (PARTITION BY channel, promo_code) AS channel_stddev_samp_promotion_discount_pct,
        ROUND(promotion_discount_pct / SUM(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code), 4) AS promotion_discount_pct_share
    FROM with_window
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        channel_stddev_samp_promotion_discount_pct / NULLIF(promotion_discount_pct_share, 0) AS channel_stddev_samp_promotion_discount_pct_per_promotion_discount_pct_share,
        LENGTH(channel) AS channel_length
    FROM with_windows
    WHERE starts_on >= DATE '2023-09-01'
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        channel_stddev_samp_promotion_discount_pct,
        promotion_discount_pct_share,
        channel_stddev_samp_promotion_discount_pct_per_promotion_discount_pct_share,
        UPPER(promotion_promo_code) AS promotion_promo_code_upper,
        DATE_ADD(ends_on, 31) AS ends_on_plus_31d,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM base
    WHERE ends_on >= DATE '2024-05-10'
      AND promotion_promo_code IS NOT NULL
      AND ends_on < '2023-06-24'
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    promo_id,
    promotion_discount_pct_share,
    channel_stddev_samp_promotion_discount_pct_per_promotion_discount_pct_share,
    ends_on_plus_31d,
    resolved_channel
FROM base_promotions
ORDER BY starts_on DESC;
