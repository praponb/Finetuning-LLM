WITH cleaned AS (
    SELECT
        *,
        LOWER(segment) segment_lower,
        LENGTH(country) country_length
    FROM customers
    WHERE segment IS NOT NULL
),
filtered AS (
    SELECT
        *,
        TRIM(city) clean_city,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') signup_date_label,
        COALESCE(city, 'NONE') city_filled
    FROM cleaned
    WHERE signup_date > DATE '2023-07-16'
      AND city IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        email,
        city,
        credit_limit,
        country_length,
        clean_city,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_credit_limit,
        RANK() OVER (PARTITION BY segment ORDER BY age NULLS LAST) age_rank
    FROM filtered
),
metrics AS (
    SELECT
        id,
        city,
        country_length,
        clean_city,
        age_rank,
        SUM(running_credit_limit) OVER (PARTITION BY city) city_sum_running_credit_limit,
        NTILE(4) OVER (PARTITION BY city ORDER BY running_credit_limit, id) running_credit_limit_bucket
    FROM with_windows
)
SELECT
    city,
    PERCENTILE_APPROX(country_length, ARRAY(0.25, 0.5, 0.75)) AS country_length_quartiles,
    SUM(city_sum_running_credit_limit) FILTER (WHERE city = 'Tokyo') AS tokyo_total_city_sum_running_credit_limit,
    PERCENTILE_APPROX(running_credit_limit_bucket, 0.5) AS running_credit_limit_bucket_p50,
    APPROX_PERCENTILE(running_credit_limit_bucket, 0.9) AS running_credit_limit_bucket_p90,
    PERCENTILE_APPROX(age_rank, 0.95) AS age_rank_p95
FROM metrics
GROUP BY city
HAVING APPROX_PERCENTILE(running_credit_limit_bucket, 0.9) > 0;
