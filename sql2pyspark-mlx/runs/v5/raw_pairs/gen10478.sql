WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(end_date, start_date) days_start_date_to_end_date,
        DATE_FORMAT(end_date, 'yyyy-MM') end_date_label
    FROM subscriptions
    WHERE start_date BETWEEN '2023-08-01' AND '2025-06-01'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT plan, id
    FROM filtered
    INTERSECT ALL
    SELECT plan, id
    FROM subscriptions
    WHERE start_date BETWEEN '2024-05-01' AND '2025-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
),
filtered_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        days_start_date_to_end_date,
        end_date_label,
        LENGTH(plan) plan_length
    FROM verified
    WHERE days_start_date_to_end_date >= 33
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        days_start_date_to_end_date,
        end_date_label,
        plan_length,
        COALESCE(monthly_fee, 0) monthly_fee_filled
    FROM filtered_subscriptions
    WHERE monthly_fee > 120
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) monthly_fee_share,
        DENSE_RANK() OVER w_hist monthly_fee_dense_rank
    FROM valid
    WHERE plan = 'basic'
    WINDOW w_hist AS (ORDER BY monthly_fee NULLS LAST)
)
-- Join a dimension and aggregate
SELECT
    d.country,
    s.plan,
    COUNT(*) AS cnt,
    AVG(s.monthly_fee_share) AS avg_monthly_fee_share,
    MAX(s.plan_length) AS max_plan_length,
    GROUPING_ID() AS grouping_level
FROM scored s
LEFT JOIN customers d ON s.customer_id = d.id
GROUP BY GROUPING SETS ((d.country, s.plan), (d.country))
