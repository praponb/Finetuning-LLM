WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(start_date, end_date) days_end_date_to_start_date,
        DATEDIFF(CURRENT_DATE(), start_date) days_since_start_date
    FROM subscriptions
    WHERE start_date >= DATE '2023-05-24'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name,
        p.country,
        p.age
    FROM valid s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY days_since_start_date, id) seq_no,
        AVG(days_end_date_to_start_date) OVER (PARTITION BY status) status_avg_days_end_date_to_start_date,
        LAG(age, 1, 0) OVER (ORDER BY days_end_date_to_start_date, id) prev_age,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
        SUM(monthly_fee) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_monthly_fee
    FROM enriched
    WHERE plan = 'premium'
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        start_date,
        end_date,
        days_end_date_to_start_date,
        country,
        age,
        prev_age,
        running_monthly_fee,
        LEAD(age) OVER (PARTITION BY country, plan ORDER BY seq_no, id) next_age,
        LAG(first_known_monthly_fee) OVER (PARTITION BY country, plan ORDER BY seq_no, id) prev_first_known_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY country, plan ORDER BY seq_no, id) row_seq
    FROM metrics
)
-- Window metrics over several different partitions
SELECT
    id,
    start_date,
    days_end_date_to_start_date,
    age,
    prev_age,
    running_monthly_fee,
    MIN(days_end_date_to_start_date) OVER (PARTITION BY plan) plan_min_days_end_date_to_start_date,
    LEAD(running_monthly_fee, 1) OVER w_ordered next_running_monthly_fee
FROM enriched_subscriptions
WINDOW w_ordered AS (ORDER BY start_date, id)
