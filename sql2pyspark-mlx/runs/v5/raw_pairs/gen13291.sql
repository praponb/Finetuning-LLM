SELECT
    o.id,
    o.discount,
    s.store_name,
    c.credit_limit,
    c.signup_date
FROM orders o
LEFT OUTER JOIN stores s ON s.id = o.store_id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
WHERE s.sqft <> 9762;
