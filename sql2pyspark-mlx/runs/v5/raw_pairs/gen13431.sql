SELECT
    *,
    LAG(credit_limit) OVER w_part prev_credit_limit,
    LAG(age) OVER w_part prev_age,
    ROW_NUMBER() OVER w_part row_seq
FROM customers
WINDOW w_part AS (PARTITION BY city ORDER BY signup_date, id)
