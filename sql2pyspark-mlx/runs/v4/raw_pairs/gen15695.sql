WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        CAST(credit_limit AS INT) credit_limit_int
    FROM customers
    WHERE segment IS NOT NULL
      AND credit_limit BETWEEN 10800 AND 12200
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit_int DESC, id) row_seq,
        NTILE(4) OVER (PARTITION BY segment, country ORDER BY credit_limit_int, id) credit_limit_int_bucket,
        SUM(age) OVER (PARTITION BY city) city_sum_age,
        CORR(age, credit_limit) OVER (PARTITION BY city) corr_age_credit_limit,
        SUM(age) OVER (PARTITION BY segment ORDER BY credit_limit_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_age
    FROM valid
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city, segment ORDER BY credit_limit_int_bucket NULLS LAST) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        credit_limit_int,
        row_seq,
        credit_limit_int_bucket,
        city_sum_age,
        corr_age_credit_limit,
        running_age,
        latest_rank,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY corr_age_credit_limit NULLS LAST) corr_age_credit_limit_dense_rank,
        LEAD(credit_limit_int_bucket, 1) OVER (PARTITION BY city ORDER BY credit_limit_int_bucket, id) next_credit_limit_int_bucket,
        LEAD(credit_limit_int, 2) OVER (PARTITION BY segment, city ORDER BY credit_limit, id) next_credit_limit_int,
        LEAD(row_seq, 1) OVER (PARTITION BY country ORDER BY latest_rank, id) next_row_seq
    FROM latest
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date >= '2023-03-01'
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        credit_limit_int,
        credit_limit_int_bucket,
        city_sum_age,
        corr_age_credit_limit,
        running_age,
        corr_age_credit_limit_dense_rank,
        next_credit_limit_int_bucket,
        next_row_seq,
        credit_limit_int * next_credit_limit_int_bucket credit_limit_int_times_next_credit_limit_int_bucket
    FROM not_flagged
    WHERE segment IS NOT NULL
      AND city_sum_age >= 254
),
with_windows_customers AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(credit_limit_int_times_next_credit_limit_int_bucket) OVER (PARTITION BY city ORDER BY corr_age_credit_limit_dense_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) credit_limit_int_times_next_credit_limit_int_bucket_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY city ORDER BY credit_limit_int_bucket DESC) credit_limit_int_bucket_cume_dist
    FROM filtered
)
SELECT
    id,
    city,
    segment,
    is_active,
    age,
    city_sum_age,
    corr_age_credit_limit,
    corr_age_credit_limit_dense_rank,
    next_credit_limit_int_bucket,
    CASE WHEN city RLIKE '^(Singapore|Chiang Mai)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS city_zone
FROM with_windows_customers
ORDER BY corr_age_credit_limit_dense_rank DESC NULLS FIRST
