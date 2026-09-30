SELECT
    id,
    credit_limit,
    country,
    signup_date,
    FIRST_VALUE(country) OVER (ORDER BY credit_limit DESC, id) AS first_country,
    DENSE_RANK() OVER (ORDER BY signup_date) AS signup_date_dense_rank
FROM customers
WHERE segment IN ('Consumer', 'SMB')
