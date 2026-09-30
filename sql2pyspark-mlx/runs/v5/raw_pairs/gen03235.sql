WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        ADD_MONTHS(start_date, 1) AS start_date_plus_months
    FROM subscriptions
    WHERE start_date > DATE '2023-05-25'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        start_date_plus_months,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id) AS seq_no
    FROM valid
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM metrics a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date_plus_months >= sub.start_date
       AND a.start_date_plus_months < sub.end_date + INTERVAL '7' DAY
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee
    FROM matched e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date_plus_months BETWEEN sub.start_date AND sub.end_date
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date_plus_months NULLS LAST, id, subscription_id, subscription_id_2) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY subscription_plan ORDER BY monthly_fee NULLS LAST) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', start_date_plus_months) AS quarter_start,
    subscription_plan_2,
    COUNT(DISTINCT plan) AS distinct_plans,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN subscription_plan_2 = 'enterprise' THEN 1 END), 0), 2) AS seq_no_per_enterprise,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    SUM(CASE WHEN plan = 'standard' THEN subscription_monthly_fee ELSE 0 END) AS standard_subscription_monthly_fee
FROM latest
GROUP BY 1, 2
HAVING ROUND(AVG(seq_no), 2) >= 0
ORDER BY seq_no_per_enterprise NULLS LAST;
