WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(city, 1, 4) AS city_prefix
    FROM customers
    WHERE segment IS NOT NULL
),
scored AS (
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
        city_prefix,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY age DESC) AS age_percent_rank,
        RANK() OVER (PARTITION BY country, city ORDER BY credit_limit DESC) AS credit_limit_rank,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share,
        FIRST_VALUE(city) IGNORE NULLS OVER w_ordered AS first_known_city
    FROM base
    WINDOW w_ordered AS (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        city,
        signup_date,
        age,
        age_percent_rank,
        first_known_credit_limit,
        first_known_city,
        CASE WHEN country IN ('SG', 'TH') THEN 'Group A' ELSE 'Other' END AS country_group,
        COALESCE(city, 'OTHER') AS city_filled
    FROM scored
    WHERE signup_date > DATE '2024-04-12'
      AND city IS NOT NULL
),
scored_customers AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY country ORDER BY age_percent_rank DESC, id) AS age_percent_rank_bucket,
        NTILE(5) OVER (ORDER BY first_known_credit_limit DESC, id) AS first_known_credit_limit_bucket
    FROM filtered
)
-- Grouped metrics
SELECT
    country,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    SUM(CASE WHEN country = 'SG' THEN age ELSE 0 END) AS sg_age,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN country = 'JP' THEN 1 END), 0), 2) AS age_per_jp,
    BIT_OR(age) AS age_bit_or,
    COUNT(*) AS event_count,
    SUM(first_known_credit_limit) FILTER (WHERE country = 'DE') AS de_total_first_known_credit_limit,
    MAX(first_known_credit_limit) AS max_first_known_credit_limit
FROM scored_customers
GROUP BY CUBE (country, DATE_TRUNC('YEAR', signup_date))
HAVING age_bit_or > 1
ORDER BY sg_age DESC
