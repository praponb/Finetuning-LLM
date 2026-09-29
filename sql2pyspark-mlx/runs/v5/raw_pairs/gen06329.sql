WITH deduped AS (
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) = 1
),
valid AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        ROUND(monthly_fee * 0.85, 2) AS monthly_fee_adjusted
    FROM deduped
    WHERE customer_id IS NOT NULL
      AND plan = 'basic'
),
joined AS (
    SELECT
        e.*,
        b.credit_limit,
        b.city,
        b.age
    FROM valid e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
filtered AS (
    SELECT
        *,
        CASE WHEN age >= 65 THEN 'High' WHEN age >= 26 THEN 'Medium' ELSE 'Low' END AS age_level,
        COALESCE(city, 'NONE') AS resolved_city
    FROM joined
    WHERE start_date >= DATE '2023-10-01'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, city ORDER BY credit_limit DESC NULLS FIRST, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
scored AS (
    SELECT
        id,
        plan,
        monthly_fee,
        credit_limit,
        age,
        age_level,
        resolved_city,
        COUNT(monthly_fee_adjusted) OVER w_part AS running_count_monthly_fee_adjusted,
        SUM(monthly_fee_adjusted) OVER w_part AS running_sum_monthly_fee_adjusted,
        MIN(monthly_fee_adjusted) OVER w_part AS running_min_monthly_fee_adjusted,
        MAX(monthly_fee_adjusted) OVER w_part AS running_max_monthly_fee_adjusted,
        FIRST_VALUE(credit_limit, TRUE) OVER (ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit
    FROM ranked
    WHERE age BETWEEN 40 AND 47
    WINDOW w_part AS (PARTITION BY plan ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        plan,
        monthly_fee,
        credit_limit,
        age,
        age_level,
        resolved_city,
        running_count_monthly_fee_adjusted,
        running_sum_monthly_fee_adjusted,
        running_min_monthly_fee_adjusted,
        running_max_monthly_fee_adjusted,
        first_known_credit_limit,
        CASE WHEN running_sum_monthly_fee_adjusted >= 320 THEN 'High' WHEN running_sum_monthly_fee_adjusted >= 90 THEN 'Medium' ELSE 'Low' END AS running_sum_monthly_fee_adjusted_band,
        monthly_fee - age AS monthly_fee_minus_age
    FROM scored
    WHERE plan = 'enterprise'
)
SELECT
    CASE WHEN first_known_credit_limit >= 14900 THEN 'LARGE' ELSE 'SMALL' END AS first_known_credit_limit_tier,
    plan,
    CASE WHEN running_min_monthly_fee_adjusted >= 79 THEN 'LARGE' ELSE 'SMALL' END AS running_min_monthly_fee_adjusted_tier,
    COUNT_IF(plan = 'premium') AS premium_events,
    APPROX_PERCENTILE(running_min_monthly_fee_adjusted, 0.5) AS running_min_monthly_fee_adjusted_p50,
    AVG(credit_limit) AS avg_credit_limit,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN first_known_credit_limit >= 14900 THEN 'LARGE' ELSE 'SMALL' END ORDER BY AVG(credit_limit) DESC) AS avg_credit_limit_dense_rank
FROM cleaned
GROUP BY 1, 2, 3
