WITH base AS (
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic
    FROM subscriptions
    WHERE monthly_fee IS NOT NULL
)
SELECT
    src.*,
    sub.id AS subscription_id,
    sub.plan AS subscription_plan,
    sub.monthly_fee AS subscription_monthly_fee
FROM base src
LEFT JOIN subscriptions sub
    ON src.customer_id = sub.customer_id
   AND src.end_date >= sub.start_date
   AND src.end_date <= DATE_ADD(sub.end_date, 3);
