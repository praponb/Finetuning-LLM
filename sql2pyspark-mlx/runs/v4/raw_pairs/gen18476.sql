SELECT
    e.*,
    p.signup_date,
    p.name,
    p.age
FROM sessions e
LEFT JOIN customers p
    ON e.customer_id = p.id
