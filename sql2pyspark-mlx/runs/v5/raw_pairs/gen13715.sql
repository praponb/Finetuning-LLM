SELECT
    id,
    signup_date,
    segment,
    CASE WHEN age >= 56 THEN 'High' WHEN age >= 25 THEN 'Medium' ELSE 'Low' END AS age_level,
    INITCAP(TRIM(country)) AS country_title
FROM customers
WHERE segment = 'SMB' AND city IS NOT NULL AND credit_limit >= 15100;
