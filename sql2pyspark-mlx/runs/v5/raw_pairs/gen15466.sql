WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        DATE_TRUNC('MONTH', signup_date) AS signup_date_period,
        CONCAT(name, ', ', segment) AS name_segment,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM customers
    WHERE segment IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    name,
    country,
    city,
    age,
    SUM(credit_limit) OVER w_recent AS running_sum_credit_limit,
    AVG(credit_limit) OVER w_recent AS running_avg_credit_limit,
    MIN(credit_limit) OVER w_recent AS running_min_credit_limit,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) AS event_seq
FROM cleaned
WINDOW w_recent AS (PARTITION BY city ORDER BY signup_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
