WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on > TO_DATE('2023-08-02')
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id) AS row_num
        FROM filtered
    ) t
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ends_on,
        AVG(days_since_ends_on) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_ends_on_avg_2,
        SUM(days_since_ends_on) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_ends_on,
        LEAD(days_since_ends_on, 1) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id) AS next_days_since_ends_on
    FROM ranked
    WHERE channel IN ('mobile', 'web', 'partner')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        promo_code,
        ends_on,
        days_since_ends_on_avg_2,
        running_days_since_ends_on,
        COUNT(days_since_ends_on_avg_2) OVER w_running AS last3_count_days_since_ends_on_avg_2,
        MAX(days_since_ends_on_avg_2) OVER w_running AS last3_max_days_since_ends_on_avg_2,
        MIN(days_since_ends_on_avg_2) OVER w_running AS last3_min_days_since_ends_on_avg_2,
        SUM(days_since_ends_on_avg_2) OVER w_running AS last3_sum_days_since_ends_on_avg_2
    FROM enriched
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY days_since_ends_on_avg_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last3_max_days_since_ends_on_avg_2) OVER (PARTITION BY promo_code ORDER BY days_since_ends_on_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last3_max_days_since_ends_on_avg_2,
        SUM(last3_sum_days_since_ends_on_avg_2) OVER (PARTITION BY promo_code ORDER BY last3_max_days_since_ends_on_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last3_sum_days_since_ends_on_avg_2,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) AS row_seq
    FROM scored
)
-- Multi-level aggregation
SELECT
    promo_code,
    APPROX_PERCENTILE(last3_min_days_since_ends_on_avg_2, ARRAY(0.25, 0.5, 0.75)) AS last3_min_days_since_ends_on_avg_2_quartiles,
    COUNT(*) FILTER (WHERE promo_code = 'SPRING10') AS spring10_rows,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes
FROM enriched_promotions
GROUP BY promo_code
ORDER BY distinct_promo_codes;
