WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(email, 1, 3) AS email_prefix,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM customers
    WHERE signup_date >= TO_DATE('2023-03-09')
      AND segment IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(segment) AS segment_length
    FROM base
    WHERE signup_date >= DATE '2023-03-07'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS FIRST, id) AS latest_rank
        FROM valid
    ) t
    WHERE latest_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(credit_limit) OVER w_part AS segment_stddev_samp_credit_limit,
        AVG(segment_length) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS segment_length_avg_6
    FROM deduped
    WINDOW w_part AS (PARTITION BY segment)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        PERCENTILE_APPROX(age, 0.95) AS age_p95,
        ROUND(AVG(segment_length), 2) AS avg_segment_length,
        COUNT_IF(country = 'JP') AS jp_events
    FROM scored
    GROUP BY DATE_TRUNC('WEEK', signup_date)
)
SELECT
    week_start,
    avg_segment_length,
    jp_events
FROM summary
ORDER BY week_start NULLS FIRST;
