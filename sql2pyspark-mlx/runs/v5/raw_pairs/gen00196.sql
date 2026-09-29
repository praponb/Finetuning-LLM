WITH filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM subscriptions
    WHERE start_date >= DATE '2023-09-13'
      AND customer_id IS NOT NULL
),
filtered_subscriptions AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status_prefix,
        CASE WHEN plan IN ('basic', 'standard') THEN 'Core' ELSE 'Other' END AS plan_group
    FROM filtered
    WHERE end_date >= DATE '2023-06-12'
      AND customer_id IS NOT NULL
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY end_date DESC NULLS FIRST, id) AS row_num
    FROM filtered_subscriptions
    QUALIFY row_num = 1
),
enriched AS (
    SELECT
        e.*,
        c.name,
        c.age
    FROM deduped e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
with_dims AS (
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.end_date,
        a.status_prefix,
        c.credit_limit,
        c.name AS customer_name,
        c.signup_date
    FROM enriched a
    JOIN customers c
        ON a.customer_id <=> c.id
    WHERE c.name LIKE 'bob%'
)
SELECT
    d.city,
    src.plan,
    COUNT(*) AS num_rows,
    MIN(src.credit_limit) AS min_credit_limit,
    GROUPING_ID() AS gid
FROM with_dims src
JOIN customers d ON src.customer_id = d.id
GROUP BY ROLLUP (d.city, src.plan)
ORDER BY num_rows NULLS FIRST
