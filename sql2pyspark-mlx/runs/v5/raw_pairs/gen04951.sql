WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM subscriptions
    WHERE end_date > DATE '2023-05-03'
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT plan, customer_id
    FROM base
    INTERSECT
    SELECT plan, customer_id
    FROM subscriptions
    WHERE start_date BETWEEN '2023-11-01' AND '2024-02-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.customer_id = k.customer_id
),
scored AS (
    -- Running and moving aggregates
    SELECT *, AVG(monthly_fee) OVER (PARTITION BY customer_id, plan) AS customer_id_avg_monthly_fee
    FROM verified
    WHERE plan IN ('basic', 'standard')
),
scored_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(customer_id_avg_monthly_fee / SUM(customer_id_avg_monthly_fee) OVER (PARTITION BY plan), 4) AS customer_id_avg_monthly_fee_share,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank
    FROM scored
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.city,
        p.credit_limit
    FROM scored_subscriptions e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN customer_id_avg_monthly_fee >= 243 THEN 'HIGH' ELSE 'LOW' END AS customer_id_avg_monthly_fee_tier,
        COUNT_IF(city = 'New York') AS new_york_events,
        SUM(CASE WHEN plan = 'standard' THEN customer_id_avg_monthly_fee_share ELSE 0 END) AS standard_customer_id_avg_monthly_fee_share
    FROM with_dims
    GROUP BY CUBE (CASE WHEN customer_id_avg_monthly_fee >= 243 THEN 'HIGH' ELSE 'LOW' END)
    HAVING new_york_events >= 1
)
SELECT
    customer_id_avg_monthly_fee_tier,
    new_york_events,
    standard_customer_id_avg_monthly_fee_share
FROM daily
ORDER BY standard_customer_id_avg_monthly_fee_share ASC NULLS FIRST;
