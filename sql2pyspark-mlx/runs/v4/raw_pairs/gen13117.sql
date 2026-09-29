WITH scored AS (
    SELECT
        *,
        LEAD(age, 1) OVER (PARTITION BY segment ORDER BY signup_date ASC, id ASC) AS next_age,
        LEAD(age, 1, 0) OVER (ORDER BY credit_limit ASC, id ASC) AS next_age_2,
        RANK() OVER (PARTITION BY country ORDER BY age DESC NULLS LAST) AS age_rank,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY age ASC, id ASC) AS row_seq
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
FROM scored
ORDER BY id DESC NULLS LAST, country ASC
LIMIT 20
