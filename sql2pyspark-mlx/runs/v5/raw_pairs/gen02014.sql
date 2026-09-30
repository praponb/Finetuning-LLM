WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(credit_limit) OVER (PARTITION BY segment, city ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_3_prior,
        AVG(age) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS age_avg_2_prior,
        CORR(age, credit_limit) OVER (PARTITION BY city, country) AS corr_age_credit_limit,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS credit_limit_percent_rank,
        NTILE(5) OVER (PARTITION BY city ORDER BY age DESC, id) AS age_bucket
    FROM customers
    WHERE city <> 'Bangkok'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment ORDER BY age DESC NULLS FIRST) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT segment, city
    FROM deduped
    EXCEPT
    SELECT segment, city
    FROM customers
    WHERE email IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.segment = k.segment AND s.city = k.city
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        age_avg_2_prior,
        corr_age_credit_limit,
        credit_limit_percent_rank,
        age_bucket,
        row_num,
        COALESCE(credit_limit, 0) AS credit_limit_filled
    FROM kept_rows
    WHERE segment IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age_avg_2_prior >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_avg_2_prior_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(DISTINCT country) AS distinct_countries
FROM valid
GROUP BY age_avg_2_prior_tier
HAVING distinct_cities < 1000
ORDER BY distinct_cities DESC
