WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(status) AS status_lower
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-04-24')
      AND plan = 'standard'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        end_date,
        status,
        status_lower,
        SUM(monthly_fee) OVER w_hist AS running_monthly_fee,
        LAG(monthly_fee, 1) OVER w_ordered AS prev_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee,
        LAG(monthly_fee, 1) OVER (ORDER BY start_date, id) AS prev_monthly_fee_2
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY plan ORDER BY end_date, id)
),
cleaned_subscriptions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        end_date,
        status,
        running_monthly_fee,
        prev_monthly_fee,
        prev_monthly_fee_2,
        ROUND(running_monthly_fee, 0) AS running_monthly_fee_rounded,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM enriched
    WHERE end_date >= TO_DATE('2023-11-09')
      AND end_date <= '2025-04-01'
),
cleaned_subscriptions_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        status,
        running_monthly_fee,
        prev_monthly_fee,
        prev_monthly_fee_2,
        running_monthly_fee_rounded,
        status_prefix,
        running_monthly_fee + running_monthly_fee_rounded AS running_monthly_fee_plus_running_monthly_fee_rounded,
        COALESCE(running_monthly_fee_rounded, 0) AS running_monthly_fee_rounded_filled
    FROM cleaned_subscriptions
    WHERE end_date > TO_DATE('2023-06-21')
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.age,
        b.city
    FROM cleaned_subscriptions_2 a
    LEFT JOIN customers b
        ON a.customer_id = b.id
    WHERE b.country != 'UK'
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_monthly_fee >= 219 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_monthly_fee_tier,
    CASE WHEN running_monthly_fee_plus_running_monthly_fee_rounded >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_monthly_fee_plus_running_monthly_fee_rounded_tier,
    CASE WHEN prev_monthly_fee_2 >= 205 THEN 'LARGE' ELSE 'SMALL' END AS prev_monthly_fee_2_tier,
    SUM(CASE WHEN status = 'CANCELLED' THEN prev_monthly_fee_2 ELSE 0 END) AS cancelled_prev_monthly_fee_2,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(running_monthly_fee_rounded_filled), 2) AS avg_running_monthly_fee_rounded_filled,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows
FROM enriched_subscriptions
GROUP BY CASE WHEN prev_monthly_fee >= 219 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_monthly_fee_plus_running_monthly_fee_rounded >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_monthly_fee_2 >= 205 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY prev_monthly_fee_tier
