WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_ADD(ends_on, 19) AS ends_on_plus_19d
    FROM promotions
    WHERE discount_pct BETWEEN 0.25 AND 0.28
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id ASC) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        ends_on,
        discount_pct,
        ends_on_plus_19d,
        rank_in_group,
        COUNT(discount_pct) OVER w_grp AS running_count_discount_pct,
        MIN(discount_pct) OVER w_grp AS running_min_discount_pct,
        SUM(discount_pct) OVER w_grp AS running_sum_discount_pct
    FROM deduped
    WINDOW w_grp AS (PARTITION BY promo_code, channel ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(running_sum_discount_pct) OVER (ORDER BY ends_on ASC, id ASC) AS next_running_sum_discount_pct,
        LAG(running_sum_discount_pct) OVER (ORDER BY ends_on ASC, id ASC) AS prev_running_sum_discount_pct,
        LAG(running_count_discount_pct) OVER (ORDER BY ends_on ASC, id ASC) AS prev_running_count_discount_pct,
        LEAD(running_count_discount_pct) OVER (ORDER BY ends_on ASC, id ASC) AS next_running_count_discount_pct,
        ROW_NUMBER() OVER (ORDER BY ends_on ASC, id ASC) AS row_seq,
        AVG(discount_pct) OVER (ORDER BY ends_on ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_5_prior
    FROM enriched
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount_pct_avg_5_prior) OVER (ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_pct_avg_5_prior,
        MIN(discount_pct_avg_5_prior) OVER (ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct_avg_5_prior,
        MAX(discount_pct_avg_5_prior) OVER (ORDER BY rank_in_group ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_discount_pct_avg_5_prior
    FROM with_windows
    WHERE next_running_sum_discount_pct >= 1.23
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', ends_on_plus_19d) AS activity_month,
    CASE WHEN next_running_count_discount_pct >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_running_count_discount_pct_tier,
    DATE_TRUNC('WEEK', ends_on_plus_19d) AS week_start,
    MIN(running_min_discount_pct) AS min_running_min_discount_pct,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(prev_running_sum_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS prev_running_sum_discount_pct_quartiles
FROM metrics
GROUP BY DATE_TRUNC('MONTH', ends_on_plus_19d), CASE WHEN next_running_count_discount_pct >= 18 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', ends_on_plus_19d)
ORDER BY next_running_count_discount_pct_tier DESC
