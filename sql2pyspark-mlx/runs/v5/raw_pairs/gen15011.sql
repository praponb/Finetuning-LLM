WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        MAX(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_max_monthly_fee,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan, customer_id) AS plan_avg_monthly_fee,
        RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_rank,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, d.segment
    FROM metrics e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN monthly_fee_rank >= 8 THEN 'High' WHEN monthly_fee_rank >= 3 THEN 'Medium' ELSE 'Low' END AS monthly_fee_rank_tier,
        CONCAT_WS('-', plan, status) AS plan_status_key
    FROM enriched
    WHERE start_date >= DATE '2023-09-16'
      AND customer_id IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        status,
        customer_id_max_monthly_fee,
        plan_avg_monthly_fee,
        monthly_fee_rank,
        segment,
        monthly_fee_rank_tier
    FROM base
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY customer_id_max_monthly_fee NULLS FIRST) = 1
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.customer_id_max_monthly_fee,
        a.plan_avg_monthly_fee,
        a.monthly_fee_rank,
        a.segment,
        b.city
    FROM ranked a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
SELECT
    id,
    customer_id_max_monthly_fee,
    plan_avg_monthly_fee,
    segment
FROM enriched_subscriptions;
