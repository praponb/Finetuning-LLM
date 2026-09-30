SELECT
    id,
    name,
    email,
    segment,
    signup_date,
    credit_limit,
    LAG(credit_limit) OVER w_running AS prev_credit_limit,
    LAG(age) OVER w_running AS prev_age,
    LEAD(age) OVER w_running AS next_age,
    ROW_NUMBER() OVER w_running AS seq_no
FROM customers
WINDOW w_running AS (PARTITION BY city ORDER BY credit_limit, id);
