WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(credit_limit / age, 2) AS credit_limit_per_age,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM customers
    WHERE segment IS DISTINCT FROM 'SMB'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        email,
        country,
        signup_date,
        credit_limit,
        credit_limit_per_age,
        days_since_signup_date,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age, id) AS row_seq,
        MIN(days_since_signup_date) OVER (PARTITION BY country) AS country_min_days_since_signup_date,
        NTILE(3) OVER (PARTITION BY city, segment ORDER BY credit_limit_per_age, id) AS credit_limit_per_age_bucket
    FROM valid
    WHERE age IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM customers
    WHERE country = 'JP'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.customer_id IS NULL
        )
),
valid_customers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        credit_limit_per_age_bucket / NULLIF(days_since_signup_date, 0) AS credit_limit_per_age_bucket_per_days_since_signup_date,
        LEAST(credit_limit_per_age, row_seq) AS least_credit_limit_per_age_row_seq
    FROM kept
    WHERE signup_date > DATE '2023-09-05'
      AND country IN ('TH', 'US', 'JP')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        signup_date,
        credit_limit,
        credit_limit_per_age,
        row_seq,
        country_min_days_since_signup_date,
        credit_limit_per_age_bucket_per_days_since_signup_date,
        SUM(days_since_signup_date) OVER w_ordered AS country_sum_days_since_signup_date,
        CUME_DIST() OVER (PARTITION BY country ORDER BY least_credit_limit_per_age_row_seq DESC NULLS LAST) AS least_credit_limit_per_age_row_seq_cume_dist,
        CUME_DIST() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS credit_limit_cume_dist,
        COUNT(credit_limit_per_age_bucket_per_days_since_signup_date) OVER w_ordered AS country_count_credit_limit_per_age_bucket_per_days_since_signup_date
    FROM valid_customers
    WINDOW w_ordered AS (PARTITION BY country)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date < s.signup_date
        )
),
daily AS (
    -- Multi-level aggregation
    SELECT
        country,
        APPROX_PERCENTILE(credit_limit_cume_dist, 0.95) AS credit_limit_cume_dist_p95,
        COUNT_IF(country = 'SG') AS sg_events,
        PERCENTILE_APPROX(credit_limit_per_age_bucket_per_days_since_signup_date, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_per_age_bucket_per_days_since_signup_date_quartiles,
        STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
        APPROX_PERCENTILE(country_count_credit_limit_per_age_bucket_per_days_since_signup_date, ARRAY(0.25, 0.5, 0.75)) AS country_count_credit_limit_per_age_bucket_per_days_since_signup_date_quartiles
    FROM not_flagged
    GROUP BY country
)
SELECT
    country,
    sg_events,
    credit_limit_per_age_bucket_per_days_since_signup_date_quartiles,
    credit_limit_stddev,
    country_count_credit_limit_per_age_bucket_per_days_since_signup_date_quartiles
FROM daily
ORDER BY country;
