WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country), 4) AS credit_limit_share,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY country) AS country_stddev_samp_credit_limit
    FROM customers
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(country)) AS country_title
    FROM scored
    WHERE signup_date >= DATE '2023-11-30'
      AND segment IS NOT NULL
      AND city IN ('Berlin', 'London')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        credit_limit_share,
        country_stddev_samp_credit_limit,
        country_title,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY segment) AS segment_stddev_samp_credit_limit,
        SUM(age) OVER w_recent AS running_age
    FROM valid
    WINDOW w_recent AS (PARTITION BY segment, city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
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
FROM metrics
ORDER BY city DESC NULLS LAST;
