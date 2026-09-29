WITH valid AS (
    SELECT *, CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END AS is_consumer
    FROM customers
    WHERE city IS DISTINCT FROM 'Bangkok'
      AND signup_date > TO_DATE('2023-02-11')
),
base AS (
    SELECT
        *,
        CAST(is_active AS INT) AS is_active_flag,
        is_consumer + age AS is_consumer_plus_age
    FROM valid
    WHERE signup_date >= DATE '2023-06-15'
      AND segment IS NOT NULL
),
filtered AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        is_consumer,
        is_consumer_plus_age,
        ROUND(credit_limit, 0) AS credit_limit_rounded
    FROM base
    WHERE signup_date >= TO_DATE('2023-02-19')
),
valid_customers AS (
    SELECT *, CAST(is_consumer_plus_age AS INT) AS is_consumer_plus_age_int
    FROM filtered
    WHERE signup_date > DATE '2023-03-04'
),
latest_match AS (
    SELECT s.*, top1.status
    FROM valid_customers s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    segment,
    metric_name,
    reading
FROM (
    SELECT
        id,
        segment,
        credit_limit,
        credit_limit_rounded
    FROM latest_match
) src
UNPIVOT (
    reading FOR metric_name IN (credit_limit AS CREDIT_LIMIT_VAL, credit_limit_rounded AS CREDIT_LIMIT_ROUNDED)
)
