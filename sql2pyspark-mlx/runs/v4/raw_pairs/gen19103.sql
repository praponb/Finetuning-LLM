SELECT
    id,
    signup_date,
    age,
    LEAD(age, 1) OVER (ORDER BY signup_date, id) AS next_age
FROM customers
ORDER BY id DESC, signup_date
