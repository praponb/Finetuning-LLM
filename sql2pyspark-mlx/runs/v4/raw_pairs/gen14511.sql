WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATE_TRUNC('year', end_date) AS end_date_period,
        ADD_MONTHS(end_date, 1) AS end_date_plus_months
    FROM subscriptions
    WHERE end_date > DATE '2023-09-23'
      AND customer_id IS NOT NULL
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM base
    INTERSECT
    SELECT customer_id
    FROM subscriptions
    WHERE end_date BETWEEN '2024-04-01' AND '2024-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.customer_id = k.customer_id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY status) AS status_sum_monthly_fee,
        RANK() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY CAST(end_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_7d
    FROM verified
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.name,
        d.city
    FROM scored s
    LEFT JOIN customers d
        ON s.customer_id = d.id
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    end_date_period,
    end_date_plus_months,
    status_sum_monthly_fee,
    monthly_fee_rank,
    monthly_fee_avg_7d,
    name,
    city,
    RANK() OVER (ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_rank_2,
    SUM(status_sum_monthly_fee) OVER (ORDER BY CAST(end_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS status_sum_monthly_fee_sum_7d,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share
FROM with_dims
