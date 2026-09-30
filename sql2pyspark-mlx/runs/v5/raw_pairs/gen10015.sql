WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_SUB(ends_on, 61) AS ends_on_minus_61d
    FROM promotions
    WHERE channel IS NOT NULL
      AND starts_on < '2025-04-06'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ADD_MONTHS(starts_on, 3) AS starts_on_plus_months,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM filtered
    WHERE starts_on > DATE '2023-03-20'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        starts_on,
        ends_on,
        ends_on_minus_61d,
        starts_on_plus_months,
        channel_clean,
        LEAD(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS next_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS seq_no
    FROM valid
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        starts_on,
        ends_on,
        ends_on_minus_61d,
        starts_on_plus_months,
        seq_no,
        AVG(seq_no) OVER (ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_seq_no,
        SUM(seq_no) OVER (ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_seq_no,
        MIN(seq_no) OVER (ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_seq_no,
        AVG(seq_no) OVER (ORDER BY seq_no, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_avg_2_prior
    FROM with_windows
    WHERE seq_no IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(last7_avg_seq_no) OVER w_recent AS running_count_last7_avg_seq_no,
        MAX(last7_avg_seq_no) OVER w_recent AS running_max_last7_avg_seq_no,
        MIN(last7_avg_seq_no) OVER w_recent AS running_min_last7_avg_seq_no,
        SUM(last7_avg_seq_no) OVER w_recent AS running_sum_last7_avg_seq_no
    FROM scored
    WINDOW w_recent AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', starts_on_plus_months) AS year_start,
    DATE_TRUNC('WEEK', starts_on_plus_months) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(seq_no_avg_2_prior, ARRAY(0.25, 0.5, 0.75)) AS seq_no_avg_2_prior_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(running_max_last7_avg_seq_no, ARRAY(0.25, 0.5, 0.75)) AS running_max_last7_avg_seq_no_quartiles
FROM metrics
GROUP BY CUBE (DATE_TRUNC('YEAR', starts_on_plus_months), DATE_TRUNC('WEEK', starts_on_plus_months))
HAVING distinct_ids_2 > 1
ORDER BY distinct_ids DESC;
