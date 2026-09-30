WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        TRIM(status) clean_status
    FROM subscriptions
    WHERE end_date >= TO_DATE('2024-03-14')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM base src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date BETWEEN sub.start_date AND sub.end_date
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        clean_status,
        subscription_id,
        subscription_monthly_fee,
        UPPER(plan) plan_upper,
        ADD_MONTHS(start_date, 3) start_date_plus_months
    FROM attributed
    WHERE start_date > DATE '2024-01-31'
      AND subscription_plan IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        subscription_monthly_fee
    FROM base_subscriptions
) src
PIVOT (
    AVG(subscription_monthly_fee)
    FOR status IN ('EXPIRED', 'PAUSED', 'CANCELLED')
)
