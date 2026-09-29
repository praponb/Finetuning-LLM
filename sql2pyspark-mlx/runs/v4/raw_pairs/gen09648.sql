WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(name, ' ', country) AS name_country,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM customers
    WHERE signup_date > TO_DATE('2024-02-22')
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS(',', name, segment) AS name_segment_key
    FROM valid
    WHERE segment IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY segment) AS segment_stddev_samp_credit_limit,
        LEAD(age, 1) OVER (PARTITION BY segment ORDER BY signup_date, id) AS next_age,
        LAG(age, 2, 0) OVER (PARTITION BY country ORDER BY signup_date, id) AS prev_age,
        FIRST_VALUE(credit_limit, TRUE) OVER (PARTITION BY country, segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit
    FROM base
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date NULLS LAST, id) = 1
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit
FROM ranked;
