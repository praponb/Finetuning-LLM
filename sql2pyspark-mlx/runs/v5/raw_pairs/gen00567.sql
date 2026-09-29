WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        IF(is_active, 'Yes', 'No') AS is_active_label
    FROM customers
    WHERE segment IS DISTINCT FROM 'SMB'
      AND segment IN ('SMB', 'Consumer')
),
base_customers AS (
    SELECT
        *,
        INITCAP(TRIM(email)) AS email_title,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM base
    WHERE city IS DISTINCT FROM 'Singapore'
),
eligible AS (
    SELECT s.*
    FROM base_customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.status <> 'ACTIVE'
        )
),
matched_keys AS (
    SELECT country
    FROM eligible
    INTERSECT ALL
    SELECT country
    FROM customers
    WHERE city NOT IN ('Tokyo', 'Chiang Mai')
),
reconciled AS (
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.country = k.country
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY city, country ORDER BY credit_limit) AS credit_limit_cume_dist,
        CORR(age, credit_limit) OVER (PARTITION BY city) AS corr_age_credit_limit
    FROM reconciled
),
filtered AS (
    SELECT *, CASE WHEN country IN ('UK', 'JP') THEN 'Group A' ELSE 'Other' END AS country_group
    FROM metrics
    WHERE signup_date >= DATE '2023-08-18'
      AND country <> 'JP'
)
SELECT
    id,
    email,
    country,
    city,
    signup_date,
    credit_limit,
    age,
    is_active_label,
    email_title,
    CASE WHEN city RLIKE '^(Tokyo|London)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone
FROM filtered
ORDER BY country NULLS LAST
