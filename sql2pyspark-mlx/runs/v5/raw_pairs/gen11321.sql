WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(credit_limit, age) AS greatest_credit_limit_age
    FROM customers
    WHERE city = 'Bangkok'
      AND signup_date > DATE '2023-03-29'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(age) OVER w_recent AS last7_avg_age,
        SUM(age) OVER w_recent AS last7_sum_age,
        COUNT(age) OVER w_recent AS last7_count_age,
        AVG(credit_limit) OVER (PARTITION BY city, segment) AS city_avg_credit_limit
    FROM base
    WINDOW w_recent AS (PARTITION BY city, segment ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT city
    FROM enriched
    INTERSECT
    SELECT city
    FROM customers
    WHERE signup_date >= '2024-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.city = k.city
),
enriched_customers AS (
    -- Window metrics over several different partitions
    SELECT
        name,
        email,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        greatest_credit_limit_age,
        last7_sum_age,
        last7_count_age,
        city_avg_credit_limit,
        RANK() OVER (PARTITION BY segment ORDER BY greatest_credit_limit_age NULLS LAST) AS greatest_credit_limit_age_rank,
        DENSE_RANK() OVER (ORDER BY city_avg_credit_limit DESC) AS city_avg_credit_limit_dense_rank
    FROM reconciled
    WHERE last7_sum_age > 280
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        APPROX_PERCENTILE(greatest_credit_limit_age, ARRAY(0.25, 0.5, 0.75)) AS greatest_credit_limit_age_quartiles
    FROM enriched_customers
    GROUP BY week_start
)
SELECT week_start, city_list
FROM stats
ORDER BY week_start DESC
LIMIT 20;
