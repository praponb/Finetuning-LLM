SELECT
    id,
    signup_date,
    city,
    segment,
    COALESCE(credit_limit, 0) AS credit_limit_filled,
    INITCAP(TRIM(name)) AS name_title
FROM customers
WHERE NOT is_active AND (credit_limit IS NULL OR country IN ('DE', 'US'));
