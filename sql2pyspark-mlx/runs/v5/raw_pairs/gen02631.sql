WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee
    FROM subscriptions
    QUALIFY RANK() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC NULLS LAST) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        NTILE(4) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee
    FROM latest
),
latest_subscriptions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        first_known_monthly_fee,
        monthly_fee_bucket
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY plan_avg_monthly_fee) = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.age,
        p.name
    FROM latest_subscriptions e
    LEFT JOIN customers p
        ON e.customer_id = p.id
    WHERE p.email LIKE '%@gmail.com'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(monthly_fee_bucket) OVER w_ordered AS running_count_monthly_fee_bucket,
        MAX(monthly_fee_bucket) OVER w_ordered AS running_max_monthly_fee_bucket,
        AVG(monthly_fee_bucket) OVER w_ordered AS running_avg_monthly_fee_bucket,
        MIN(monthly_fee_bucket) OVER w_ordered AS running_min_monthly_fee_bucket,
        AVG(monthly_fee_bucket) OVER w_ordered_2 AS monthly_fee_bucket_avg_3
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY customer_id, plan ORDER BY monthly_fee_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered_2 AS (ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee_bucket,
        age,
        running_count_monthly_fee_bucket,
        running_max_monthly_fee_bucket,
        running_avg_monthly_fee_bucket,
        monthly_fee_bucket_avg_3,
        UPPER(plan) AS plan_upper
    FROM scored
    WHERE start_date >= TO_DATE('2023-03-08')
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM valid
    EXCEPT
    SELECT customer_id, id
    FROM subscriptions
    WHERE end_date BETWEEN '2023-03-24' AND '2024-08-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        plan,
        monthly_fee_bucket_avg_3
    FROM kept_rows
) src
PIVOT (
    AVG(monthly_fee_bucket_avg_3)
    FOR plan IN ('standard', 'premium')
);
