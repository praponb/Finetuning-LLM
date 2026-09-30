WITH metrics AS (
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        MIN(age) OVER w_ordered AS running_min_age,
        COUNT(age) OVER w_ordered AS running_count_age,
        SUM(age) OVER w_ordered AS running_sum_age,
        MAX(age) OVER w_ordered AS running_max_age,
        ROW_NUMBER() OVER (ORDER BY age, id) AS event_seq
    FROM customers
    WHERE credit_limit < 9500
    WINDOW w_ordered AS (PARTITION BY city, country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
ranked_customers AS (
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY credit_limit NULLS FIRST, id) = 1
),
kept AS (
    SELECT s.*
    FROM ranked_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
)
SELECT
    id,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    running_min_age,
    running_count_age,
    running_sum_age,
    CASE WHEN city RLIKE '^(New York|Berlin)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS city_zone
FROM kept
ORDER BY signup_date
