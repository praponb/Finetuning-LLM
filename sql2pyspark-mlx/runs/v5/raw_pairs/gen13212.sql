WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        FLOOR(discount_pct) AS discount_pct_whole,
        ADD_MONTHS(starts_on, 3) AS starts_on_plus_months
    FROM promotions
    WHERE channel IN ('partner', 'mobile')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        discount_pct_whole,
        starts_on_plus_months,
        LEAST(discount_pct_whole, discount_pct) AS least_discount_pct_whole_discount_pct
    FROM cleaned
    WHERE channel IS DISTINCT FROM 'partner'
      AND discount_pct > 0.2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        starts_on_plus_months,
        least_discount_pct_whole_discount_pct,
        PERCENT_RANK() OVER (PARTITION BY promo_code, channel ORDER BY least_discount_pct_whole_discount_pct NULLS FIRST) AS least_discount_pct_whole_discount_pct_percent_rank,
        DENSE_RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC) AS discount_pct_dense_rank
    FROM base
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics src
    INNER JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on_plus_months >= pr.starts_on
       AND src.starts_on_plus_months < pr.ends_on + INTERVAL '14' DAY
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(discount_pct_dense_rank) OVER (PARTITION BY channel ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_discount_pct_dense_rank,
        AVG(discount_pct_dense_rank) OVER (PARTITION BY channel ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_discount_pct_dense_rank,
        SUM(discount_pct_dense_rank) OVER (PARTITION BY channel ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_discount_pct_dense_rank,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY channel, promo_code), 4) AS promo_id_share
    FROM with_window
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id, promo_id) AS dedup_rank
        FROM with_windows
    ) t
    WHERE dedup_rank = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on_plus_months >= pr.starts_on
       AND e.starts_on_plus_months <= DATE_ADD(pr.ends_on, 10)
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN promo_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_id_2_tier,
        DATE_TRUNC('QUARTER', starts_on_plus_months) AS quarter_start,
        ROUND(SUM(least_discount_pct_whole_discount_pct_percent_rank) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS least_discount_pct_whole_discount_pct_percent_rank_per_spring10,
        ROUND(SUM(least_discount_pct_whole_discount_pct_percent_rank) / NULLIF(COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END), 0), 2) AS least_discount_pct_whole_discount_pct_percent_rank_per_spring10_2,
        AVG(last4_min_discount_pct_dense_rank) FILTER (WHERE promo_code = 'SPRING10') AS spring10_avg_last4_min_discount_pct_dense_rank,
        SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes,
        COUNT(*) AS row_count
    FROM matched
    GROUP BY GROUPING SETS ((CASE WHEN promo_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', starts_on_plus_months)), (CASE WHEN promo_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END))
)
SELECT
    promo_id_2_tier,
    least_discount_pct_whole_discount_pct_percent_rank_per_spring10,
    least_discount_pct_whole_discount_pct_percent_rank_per_spring10_2,
    spring10_avg_last4_min_discount_pct_dense_rank,
    num_promotion_promo_codes
FROM grouped
ORDER BY num_promotion_promo_codes DESC NULLS FIRST
