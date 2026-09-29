WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        RANK() OVER (PARTITION BY channel ORDER BY discount_pct ASC) AS discount_pct_rank,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        NTILE(10) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC, id ASC) AS discount_pct_bucket
    FROM promotions
    WHERE starts_on BETWEEN '2025-09-01' AND '2025-10-01'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched a
    INNER JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(discount_pct, 1) AS discount_pct_rounded,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM attributed
    WHERE promotion_promo_code IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(promo_code) AS promo_code_lower,
        COALESCE(channel, 'NONE') AS channel_clean_2
    FROM cleaned
    WHERE ends_on >= DATE '2023-07-21'
      AND promotion_promo_code IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    promo_code,
    ends_on,
    discount_pct,
    discount_pct_rank,
    discount_pct_bucket,
    promo_id,
    promotion_discount_pct,
    promo_code_lower,
    channel_clean_2,
    SUM(discount_pct_rank) OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct_bucket ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_rank,
    SUM(discount_pct_rounded) OVER (PARTITION BY promo_code, channel) AS promo_code_sum_discount_pct_rounded,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_rank DESC, id ASC, promo_id ASC) AS row_seq
FROM filtered;
