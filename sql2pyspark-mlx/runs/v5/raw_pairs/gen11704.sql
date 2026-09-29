WITH filtered AS (
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        WEEKOFYEAR(ends_on) AS ends_on_weekofyear,
        DAYOFWEEK(starts_on) AS starts_on_dayofweek
    FROM promotions
    WHERE ends_on >= DATE '2023-06-23'
),
scored AS (
    SELECT
        *,
        SUM(starts_on_dayofweek) OVER (ORDER BY starts_on_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_starts_on_dayofweek,
        ROW_NUMBER() OVER (ORDER BY starts_on, id) AS seq_no,
        NTILE(4) OVER (ORDER BY ends_on_weekofyear DESC, id) AS ends_on_weekofyear_bucket,
        LAST_VALUE(ends_on_weekofyear, TRUE) OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ends_on_weekofyear
    FROM filtered
    WHERE ends_on < '2024-03-17'
),
enriched AS (
    SELECT
        *,
        LAG(ends_on_weekofyear, 1) OVER (ORDER BY seq_no, id) AS prev_ends_on_weekofyear,
        NTILE(3) OVER w_grp AS ends_on_weekofyear_bucket_2
    FROM scored
    WINDOW w_grp AS (ORDER BY ends_on_weekofyear, id)
),
base AS (
    SELECT
        *,
        COALESCE(ends_on_weekofyear_bucket, 0) AS ends_on_weekofyear_bucket_filled,
        CAST(starts_on_dayofweek AS DOUBLE) AS starts_on_dayofweek_num
    FROM enriched
    WHERE ends_on > DATE '2023-09-12'
      AND starts_on_dayofweek <= 28
),
valid AS (
    SELECT
        *,
        CAST(ends_on_weekofyear AS STRING) AS ends_on_weekofyear_num,
        LAST_DAY(starts_on) AS starts_on_month_end
    FROM base
    WHERE ends_on > TO_DATE('2023-08-18')
),
metrics AS (
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_dayofweek,
        ends_on_weekofyear_bucket,
        last_known_ends_on_weekofyear,
        prev_ends_on_weekofyear,
        ends_on_weekofyear_bucket_2,
        ends_on_weekofyear_bucket_filled,
        starts_on_dayofweek_num,
        LEAD(cumulative_starts_on_dayofweek) OVER w_part AS next_cumulative_starts_on_dayofweek,
        LEAD(ends_on_weekofyear_bucket_2) OVER w_part AS next_ends_on_weekofyear_bucket_2,
        LAG(ends_on_weekofyear_bucket_2) OVER w_part AS prev_ends_on_weekofyear_bucket_2,
        ROW_NUMBER() OVER w_part AS seq_no_2
    FROM valid
    WINDOW w_part AS (ORDER BY cumulative_starts_on_dayofweek, id)
),
grouped AS (
    SELECT
        CASE WHEN ends_on_weekofyear_bucket_filled >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS ends_on_weekofyear_bucket_filled_tier,
        CASE WHEN ends_on_weekofyear_bucket_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS ends_on_weekofyear_bucket_2_tier,
        DATE_TRUNC('YEAR', ends_on) AS year_start,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(ends_on_weekofyear_bucket, 0.95) AS ends_on_weekofyear_bucket_p95,
        COUNT(*) AS row_count_2,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics
    GROUP BY CASE WHEN ends_on_weekofyear_bucket_filled >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN ends_on_weekofyear_bucket_2 >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', ends_on)
)
SELECT
    ends_on_weekofyear_bucket_filled_tier,
    ends_on_weekofyear_bucket_2_tier,
    year_start,
    row_count,
    ends_on_weekofyear_bucket_p95,
    row_count_2
FROM grouped
ORDER BY row_count_2;
