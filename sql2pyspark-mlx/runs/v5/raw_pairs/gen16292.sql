WITH kept AS (
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < s.signup_date
        )
),
with_windows AS (
    SELECT
        id,
        name,
        email,
        country,
        signup_date,
        is_active,
        age,
        NTILE(4) OVER (PARTITION BY segment, country ORDER BY credit_limit, id) credit_limit_bucket,
        SUM(credit_limit) OVER (PARTITION BY city, country) city_sum_credit_limit,
        NTILE(5) OVER (ORDER BY age, id) age_bucket,
        CORR(credit_limit, age) OVER w_hist corr_credit_limit_age
    FROM kept
    WINDOW w_hist AS (PARTITION BY segment, country)
),
with_windows_customers AS (
    SELECT
        id,
        name,
        country,
        signup_date,
        is_active,
        credit_limit_bucket,
        city_sum_credit_limit,
        MIN(credit_limit_bucket) OVER w_running country_min_credit_limit_bucket,
        MAX(age_bucket) OVER w_running country_max_age_bucket
    FROM with_windows
    WHERE NOT is_active
    WINDOW w_running AS (PARTITION BY country)
),
deduped AS (
    SELECT
        id,
        name,
        country,
        city_sum_credit_limit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit_bucket NULLS LAST, id) AS dedup_rank
        FROM with_windows_customers
    ) ranked
    WHERE dedup_rank = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY city_sum_credit_limit DESC NULLS LAST, id) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
summary AS (
    SELECT
        CASE WHEN city_sum_credit_limit >= 85056 THEN 'HIGH' ELSE 'LOW' END AS city_sum_credit_limit_tier,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        COUNT(DISTINCT country) AS distinct_countries
    FROM latest
    GROUP BY CASE WHEN city_sum_credit_limit >= 85056 THEN 'HIGH' ELSE 'LOW' END
)
SELECT city_sum_credit_limit_tier, country_list
FROM summary
ORDER BY city_sum_credit_limit_tier DESC NULLS FIRST
