SELECT
    id,
    signup_date,
    age,
    segment,
    credit_limit,
    FIRST_VALUE(age) OVER (ORDER BY signup_date DESC, id) first_age,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) credit_limit_row
FROM customers
