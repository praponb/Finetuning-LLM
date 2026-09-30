SELECT
    id,
    city,
    country,
    age,
    DATE_ADD(signup_date, 84) AS signup_date_plus_84d,
    LOWER(segment) AS segment_lower
FROM customers
WHERE NOT (country = 'JP' AND UPPER(TRIM(email)) = 'FRANK@COMPANY.CO')
