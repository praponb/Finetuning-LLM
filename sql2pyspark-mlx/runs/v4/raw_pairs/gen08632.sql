WITH enriched AS (
    SELECT
        *,
        COUNT(age) OVER (PARTITION BY country, city) AS country_count_age,
        PERCENT_RANK() OVER (ORDER BY age NULLS LAST) AS age_percent_rank,
        FIRST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment,
        LAST_VALUE(age, TRUE) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age
    FROM customers
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    is_active,
    age,
    country_count_age,
    first_known_segment
FROM enriched
ORDER BY age DESC;
