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
    NTILE(10) OVER (PARTITION BY city ORDER BY credit_limit, id) AS credit_limit_bucket,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS row_seq
FROM customers;
