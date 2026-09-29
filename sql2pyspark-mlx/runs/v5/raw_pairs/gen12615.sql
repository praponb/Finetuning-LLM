WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(start_date, 3) AS start_date_plus_months,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE end_date >= DATE '2023-09-15'
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM filtered
    EXCEPT
    SELECT status, id
    FROM subscriptions
    WHERE YEAR(end_date) = 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
filtered_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        days_since_end_date * monthly_fee AS days_since_end_date_times_monthly_fee,
        QUARTER(start_date) AS start_date_quarter
    FROM verified
    WHERE end_date >= TO_DATE('2023-09-05')
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee,
        start_date_plus_months,
        days_since_end_date,
        days_since_end_date_times_monthly_fee,
        STDDEV_SAMP(monthly_fee) OVER (PARTITION BY status) AS status_stddev_samp_monthly_fee
    FROM filtered_subscriptions
    WHERE plan = 'standard'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', start_date_plus_months) AS week_start,
        customer_id,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        SUM(status_stddev_samp_monthly_fee) FILTER (WHERE plan = 'basic') AS basic_total_status_stddev_samp_monthly_fee,
        ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS monthly_fee_per_basic,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', start_date_plus_months) ORDER BY SIZE(COLLECT_SET(plan)) DESC) AS num_plans_percent_rank
    FROM scored
    GROUP BY week_start, customer_id
)
SELECT
    week_start,
    customer_id,
    num_plans,
    basic_total_status_stddev_samp_monthly_fee,
    monthly_fee_per_basic,
    num_plans_percent_rank
FROM totals
ORDER BY num_plans DESC, num_plans_percent_rank
