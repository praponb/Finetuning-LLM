WITH base AS (
    SELECT
        *,
        COALESCE(name, 'unassigned') name_filled,
        SPLIT(LOWER(email), '@')[0] email_user
    FROM customers
    WHERE city IS DISTINCT FROM 'New York'
      AND UPPER(name) LIKE '%ENR%'
),
matched_keys AS (
    SELECT country
    FROM base
    INTERSECT
    SELECT country
    FROM customers
    WHERE credit_limit <= 6100
),
verified AS (
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.country = k.country
),
cleaned AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        email_user,
        LOWER(email) email_lower,
        COALESCE(city, 'OTHER') resolved_city
    FROM verified
    WHERE segment IS DISTINCT FROM 'Corporate'
),
scored AS (
    SELECT
        id,
        email,
        country,
        segment,
        is_active,
        credit_limit,
        email_user,
        email_lower,
        resolved_city,
        CORR(age, credit_limit) OVER (PARTITION BY country, segment) corr_age_credit_limit
    FROM cleaned
    WHERE name LIKE '%Park%'
),
cleaned_customers AS (
    SELECT
        id,
        segment,
        is_active,
        credit_limit,
        email_user,
        email_lower,
        corr_age_credit_limit,
        COALESCE(corr_age_credit_limit, 0) corr_age_credit_limit_filled
    FROM scored
    WHERE email IS NOT NULL
)
SELECT
    id,
    segment,
    is_active,
    email_user,
    email_lower,
    corr_age_credit_limit_filled
FROM cleaned_customers;
