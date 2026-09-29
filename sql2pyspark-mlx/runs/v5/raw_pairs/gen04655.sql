WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE signup_date >= DATE '2023-08-09'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(credit_limit) OVER (PARTITION BY country, segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_credit_limit,
        SUM(credit_limit) OVER (PARTITION BY country, segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY country, segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_credit_limit,
        MIN(credit_limit) OVER (PARTITION BY country, segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
    FROM filtered
    WHERE age BETWEEN 33 AND 48
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date >= '2024-09-06'
        )
),
filtered_customers AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN age >= 61 THEN 'High' WHEN age >= 38 THEN 'Medium' ELSE 'Low' END AS age_band
    FROM eligible
    WHERE age >= 45
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.signup_date,
        s.age,
        s.running_sum_credit_limit,
        s.last_known_city,
        x.opened_date,
        x.status
    FROM filtered_customers s
    JOIN LATERAL (
            SELECT c.opened_date, c.status
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    name,
    email,
    last_known_city,
    opened_date,
    status
FROM latest_match;
