WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        LENGTH(segment) AS segment_length
    FROM customers
    WHERE signup_date >= DATE '2023-05-08'
      AND segment IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    LAST_VALUE(city, TRUE) OVER (PARTITION BY segment, country ORDER BY segment_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
    FIRST_VALUE(city) IGNORE NULLS OVER (ORDER BY segment_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
    LAG(age, 1) OVER (PARTITION BY country ORDER BY age, id) AS prev_age,
    AVG(age) OVER w_hist AS age_avg_6_prior,
    ROW_NUMBER() OVER (ORDER BY credit_limit, id) AS row_seq
FROM filtered
WINDOW w_hist AS (PARTITION BY segment, city ORDER BY segment_length, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
