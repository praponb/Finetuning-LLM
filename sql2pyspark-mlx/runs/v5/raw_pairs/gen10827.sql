WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        ROUND(credit_limit, 1) AS credit_limit_rounded,
        MONTH(signup_date) AS signup_date_month,
        COALESCE(city, 'OTHER') AS city_clean
    FROM customers
    WHERE signup_date > DATE '2023-03-30'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM filtered
    EXCEPT
    SELECT id, country
    FROM customers
    WHERE email LIKE '%@company.co'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
metrics AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY segment, country ORDER BY age ASC NULLS LAST) AS age_cume_dist
    FROM kept_rows
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        is_active,
        age,
        credit_limit_rounded,
        signup_date_month,
        city_clean,
        CUME_DIST() OVER w_ordered AS age_cume_dist_cume_dist,
        MIN(age) OVER (PARTITION BY segment) AS segment_min_age,
        MIN(age) OVER (PARTITION BY country) AS country_min_age
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY city ORDER BY age_cume_dist DESC NULLS FIRST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY credit_limit_rounded ASC) AS credit_limit_rounded_dense_rank,
        PERCENT_RANK() OVER (ORDER BY country_min_age DESC) AS country_min_age_percent_rank,
        RANK() OVER (ORDER BY credit_limit_rounded DESC NULLS FIRST) AS credit_limit_rounded_rank,
        RANK() OVER (ORDER BY segment_min_age ASC NULLS LAST) AS segment_min_age_rank
    FROM with_windows
)
SELECT
    is_active,
    age,
    credit_limit_rounded,
    signup_date_month,
    city_clean,
    age_cume_dist_cume_dist,
    segment_min_age,
    country_min_age,
    credit_limit_rounded_dense_rank
FROM scored
ORDER BY credit_limit_rounded_dense_rank DESC NULLS LAST
