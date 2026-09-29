SELECT
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    credit_limit,
    age,
    LEAD(credit_limit) OVER w_hist AS next_credit_limit,
    LEAD(age) OVER w_hist AS next_age,
    ROW_NUMBER() OVER w_hist AS row_seq,
    COUNT(credit_limit) OVER (PARTITION BY city) AS city_count_credit_limit
FROM customers
WINDOW w_hist AS (PARTITION BY segment ORDER BY credit_limit, id)
