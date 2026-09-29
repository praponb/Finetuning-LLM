WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(discount_pct, 2) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id) AS prev_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
        AVG(discount_pct) OVER (ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_2_prior
    FROM promotions
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        discount_pct_avg_2_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
ranked_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_avg_2_prior NULLS LAST, id) AS dedup_rank
        FROM ranked
    ) x
    WHERE dedup_rank = 1
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        MIN(discount_pct) OVER (PARTITION BY promo_code ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_pct
    FROM ranked_promotions
),
ranked_promotions_2 AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched_promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) = 1
),
ranked_promotions_3 AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        starts_on,
        discount_pct,
        running_min_discount_pct,
        running_sum_discount_pct
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promo_code ORDER BY starts_on DESC) AS row_num
        FROM ranked_promotions_2
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_sum_discount_pct) OVER w_part AS running_sum_discount_pct_avg_6_prior,
        FIRST_VALUE(running_sum_discount_pct) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_discount_pct
    FROM ranked_promotions_3
    WHERE YEAR(starts_on) >= 2023
    WINDOW w_part AS (ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    CASE WHEN running_min_discount_pct >= 0.28 THEN 'LARGE' ELSE 'SMALL' END AS running_min_discount_pct_tier,
    PERCENTILE_APPROX(running_sum_discount_pct_avg_6_prior, ARRAY(0.25, 0.5, 0.75)) AS running_sum_discount_pct_avg_6_prior_quartiles,
    MAX(running_min_discount_pct) AS max_running_min_discount_pct,
    ROUND(AVG(running_sum_discount_pct), 2) AS avg_running_sum_discount_pct,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', starts_on) ORDER BY ROUND(AVG(running_sum_discount_pct), 2) DESC) AS avg_running_sum_discount_pct_rank
FROM scored
GROUP BY DATE_TRUNC('WEEK', starts_on), CASE WHEN running_min_discount_pct >= 0.28 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_running_sum_discount_pct >= 0
ORDER BY max_running_min_discount_pct DESC NULLS FIRST
