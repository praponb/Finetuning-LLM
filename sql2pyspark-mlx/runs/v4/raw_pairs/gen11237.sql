WITH with_windows AS (
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
        LAST_VALUE(age, TRUE) OVER (PARTITION BY segment, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_age,
        SUM(age) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_age,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) event_seq,
        LAG(age, 1) OVER (PARTITION BY segment ORDER BY age, id) prev_age
    FROM customers
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age
FROM with_windows
