WITH filtered AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        ROUND(credit_limit / age, 2) AS credit_limit_per_age,
        LOWER(city) AS city_lower
    FROM customers
    WHERE signup_date > DATE '2023-03-18'
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date >= '2024-07-01'
        )
),
not_flagged_customers AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < '2024-07-08'
        )
),
remaining_keys AS (
    SELECT city, id
    FROM not_flagged_customers
    EXCEPT
    SELECT city, id
    FROM customers
    WHERE city IN ('Berlin', 'Tokyo')
),
kept_rows AS (
    SELECT s.*
    FROM not_flagged_customers s
    JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
remaining_keys_2 AS (
    SELECT city
    FROM kept_rows
    EXCEPT ALL
    SELECT city
    FROM customers
    WHERE city IN ('New York', 'Bangkok', 'Tokyo')
),
reconciled AS (
    SELECT s.*
    FROM kept_rows s
    JOIN remaining_keys_2 k ON s.city = k.city
)
SELECT
    CASE WHEN credit_limit_per_age >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_per_age_tier,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(SUM(credit_limit_per_age) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS credit_limit_per_age_per_new_york,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS age_per_smb
FROM reconciled
GROUP BY credit_limit_per_age_tier
ORDER BY credit_limit_per_age_per_new_york;
