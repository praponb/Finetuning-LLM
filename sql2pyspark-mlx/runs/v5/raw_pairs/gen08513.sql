WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
        MAX(monthly_fee) OVER (PARTITION BY plan) plan_max_monthly_fee
    FROM subscriptions
    WHERE monthly_fee < 190
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, d.country
    FROM metrics a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.name,
        d.email,
        d.city
    FROM with_dims s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        cumulative_monthly_fee,
        first_known_monthly_fee,
        plan_max_monthly_fee,
        email,
        city,
        GREATEST(plan_max_monthly_fee, first_known_monthly_fee) greatest_plan_max_monthly_fee_first_known_monthly_fee,
        COALESCE(city, 'UNKNOWN') city_clean
    FROM joined
    WHERE plan_max_monthly_fee > 97
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        cumulative_monthly_fee,
        first_known_monthly_fee,
        plan_max_monthly_fee,
        email,
        greatest_plan_max_monthly_fee_first_known_monthly_fee,
        city_clean,
        FIRST_VALUE(plan_max_monthly_fee) IGNORE NULLS OVER (PARTITION BY customer_id, city ORDER BY first_known_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_plan_max_monthly_fee,
        NTILE(4) OVER (ORDER BY first_known_monthly_fee DESC, id) first_known_monthly_fee_bucket,
        NTILE(10) OVER (ORDER BY plan_max_monthly_fee DESC, id) plan_max_monthly_fee_bucket
    FROM cleaned
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN plan_max_monthly_fee_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_max_monthly_fee_bucket_tier,
    COUNT(*) AS row_count,
    AVG(cumulative_monthly_fee) AS avg_cumulative_monthly_fee
FROM enriched
GROUP BY CASE WHEN plan_max_monthly_fee_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END;
