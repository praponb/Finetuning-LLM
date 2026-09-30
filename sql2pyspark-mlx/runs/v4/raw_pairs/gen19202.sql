WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS row_num
        FROM customers
    ) x
    WHERE row_num = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, REGEXP_REPLACE(name, ' ', '') AS name_slug
    FROM latest
    WHERE signup_date >= TO_DATE('2023-10-16')
),
with_windows AS (
    -- Window metrics over several different partitions
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
        row_num,
        name_slug,
        LAST_VALUE(city, TRUE) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
        DENSE_RANK() OVER w_part AS age_dense_rank,
        AVG(age) OVER w_running AS age_avg_6,
        NTILE(4) OVER w_grp AS credit_limit_bucket,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER w_grp_2 AS last_known_credit_limit
    FROM filtered
    WINDOW w_part AS (ORDER BY age DESC NULLS FIRST), w_running AS (PARTITION BY city ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY segment ORDER BY credit_limit DESC, id), w_grp_2 AS (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY credit_limit DESC) AS credit_limit_dense_rank,
        LAST_VALUE(age_avg_6, TRUE) OVER (PARTITION BY city ORDER BY last_known_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age_avg_6,
        LAST_VALUE(segment, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        AVG(age) OVER (ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS age_avg_2,
        LEAD(credit_limit_bucket, 2) OVER (PARTITION BY country, city ORDER BY row_num, id) AS next_credit_limit_bucket
    FROM with_windows
),
filtered_customers AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(age_avg_2, 2) AS age_avg_2_rounded
    FROM scored
    WHERE signup_date >= DATE '2023-05-18'
      AND city IS NOT NULL
)
SELECT
    id,
    name,
    email,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    age
FROM filtered_customers
ORDER BY credit_limit DESC NULLS FIRST, id;
