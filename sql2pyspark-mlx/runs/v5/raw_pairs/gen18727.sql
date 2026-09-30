WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        DAY(start_date) AS start_date_day
    FROM subscriptions
    WHERE MONTH(end_date) IN (1, 7, 12)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        start_date_day,
        GREATEST(monthly_fee, start_date_day) AS greatest_monthly_fee_start_date_day,
        start_date_day * monthly_fee AS start_date_day_times_monthly_fee
    FROM filtered
    WHERE customer_id IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY greatest_monthly_fee_start_date_day DESC, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
cleaned_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        start_date_day,
        greatest_monthly_fee_start_date_day,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM ranked
    WHERE start_date > DATE '2023-09-26'
),
filtered_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(plan) AS plan_upper,
        monthly_fee_whole * monthly_fee AS monthly_fee_whole_times_monthly_fee
    FROM cleaned_subscriptions
    WHERE greatest_monthly_fee_start_date_day BETWEEN 39 AND 43
),
cleaned_subscriptions_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        start_date_day,
        monthly_fee_whole,
        plan_upper,
        ROUND(monthly_fee * 1.07, 2) AS adjusted_monthly_fee
    FROM filtered_subscriptions
    WHERE start_date > DATE '2023-08-31'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        end_date,
        start_date_day,
        monthly_fee_whole,
        adjusted_monthly_fee,
        NTILE(5) OVER (ORDER BY adjusted_monthly_fee, id) AS adjusted_monthly_fee_bucket,
        AVG(monthly_fee_whole) OVER (ORDER BY start_date_day, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_whole_avg_6
    FROM cleaned_subscriptions_2
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN monthly_fee_whole_avg_6 >= 87 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_whole_avg_6_tier,
    ROUND(AVG(adjusted_monthly_fee_bucket), 2) AS avg_adjusted_monthly_fee_bucket,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(adjusted_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS adjusted_monthly_fee_quartiles,
    PERCENTILE_APPROX(start_date_day, ARRAY(0.25, 0.5, 0.75)) AS start_date_day_quartiles,
    ROUND(AVG(adjusted_monthly_fee), 2) AS avg_adjusted_monthly_fee
FROM enriched
GROUP BY CASE WHEN monthly_fee_whole_avg_6 >= 87 THEN 'HIGH' ELSE 'LOW' END
