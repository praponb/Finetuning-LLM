WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        ends_on,
        discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on DESC, id) AS latest_rank
        FROM promotions
    ) ranked
    WHERE latest_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL 7 DAYS
),
ranked_promotions AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY promotion_discount_pct DESC, id, promo_id) AS dedup_rank
    FROM with_window
    QUALIFY dedup_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_id,
        promo_code,
        promotion_discount_pct,
        dedup_rank,
        TRIM(promo_code) AS clean_promo_code
    FROM ranked_promotions
    WHERE ends_on >= TO_DATE('2023-11-03')
      AND discount_pct BETWEEN 0.13 AND 0.24
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY promotion_discount_pct DESC, id, promo_id) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_id,
        promo_code,
        clean_promo_code,
        rn,
        MIN(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY dedup_rank, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_promo_id,
        SUM(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY dedup_rank, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_promo_id,
        AVG(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY dedup_rank, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_promo_id
    FROM latest
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_id,
        clean_promo_code,
        running_min_promo_id,
        running_sum_promo_id,
        running_avg_promo_id,
        LAG(running_sum_promo_id, 1) OVER (ORDER BY running_avg_promo_id, id, promo_id) AS prev_running_sum_promo_id,
        AVG(promo_id) OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS promo_id_avg_3_prior,
        NTILE(4) OVER (ORDER BY promo_id DESC, id, promo_id) AS promo_id_bucket
    FROM enriched
)
-- Multi-level aggregation
SELECT
    promo_id,
    PERCENTILE_APPROX(prev_running_sum_promo_id, ARRAY(0.25, 0.5, 0.75)) AS prev_running_sum_promo_id_quartiles,
    APPROX_PERCENTILE(promo_id, 0.5) AS promo_id_p50,
    COUNT(*) AS cnt
FROM with_windows
GROUP BY promo_id
ORDER BY cnt DESC
