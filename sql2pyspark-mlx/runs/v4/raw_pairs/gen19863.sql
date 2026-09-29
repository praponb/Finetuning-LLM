WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee NULLS FIRST, id) AS rn
        FROM subscriptions
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        LEAD(monthly_fee, 2, 0) OVER (PARTITION BY status ORDER BY rn, id) AS next_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_2
    FROM deduped
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.city,
        d.age
    FROM enriched src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(status) AS clean_status,
        SUBSTRING(status, 1, 2) AS status_prefix
    FROM enriched_subscriptions
    WHERE end_date > DATE '2023-12-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY age DESC, id) AS rn_2
        FROM cleaned
    ) x
    WHERE rn_2 = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM ranked s
    INNER JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date >= sub.start_date
       AND s.start_date < sub.end_date + INTERVAL '7' DAY
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, subscription_monthly_fee + rn_2 AS subscription_monthly_fee_plus_rn_2
    FROM with_window
    WHERE end_date > TO_DATE('2024-04-18')
      AND subscription_plan IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    status,
    AVG(monthly_fee_avg_2) AS avg_monthly_fee_avg_2,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM base
GROUP BY status
HAVING avg_monthly_fee_avg_2 > 0;
