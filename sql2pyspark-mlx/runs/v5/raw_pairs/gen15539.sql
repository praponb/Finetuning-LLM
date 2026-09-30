WITH valid AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        CONCAT(plan, ' - ', status) AS plan_status
    FROM subscriptions
    WHERE start_date >= DATE '2023-03-29'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        src.*,
        b.segment,
        b.email
    FROM valid src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
latest AS (
    SELECT
        id,
        customer_id,
        plan
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan, customer_id ORDER BY end_date ASC NULLS LAST) AS row_num
        FROM enriched
    ) x
    WHERE row_num = 1
),
cleaned AS (
    SELECT
        *,
        LOWER(plan) AS plan_lower,
        INITCAP(TRIM(plan)) AS plan_title
    FROM latest
    WHERE customer_id = 10
),
cleaned_subscriptions AS (
    SELECT *, LOWER(plan) AS plan_lower_2
    FROM cleaned
    WHERE customer_id IS NOT NULL
      AND customer_id = 29
)
SELECT
    customer_id,
    COUNT_IF(plan = 'basic') AS basic_events,
    COUNT(*) FILTER (WHERE plan = 'standard') AS standard_rows,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt
FROM cleaned_subscriptions
GROUP BY 1
ORDER BY row_count DESC NULLS FIRST;
