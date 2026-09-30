SELECT
    src.id,
    src.customer_id,
    src.plan,
    src.end_date,
    p.name
FROM subscriptions src
LEFT JOIN customers p
    ON src.customer_id = p.id
WHERE NOT p.is_active
