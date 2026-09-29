WITH filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        DATEDIFF(CURRENT_DATE(), end_date) days_since_end_date
    FROM subscriptions
    WHERE end_date >= '2023-04-01'
),
ranked AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC NULLS LAST, id) = 1
),
with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM ranked src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date >= sub.start_date
       AND src.start_date <= DATE_ADD(sub.end_date, 10)
),
base AS (
    SELECT *, ROUND(days_since_end_date / subscription_id, 2) days_since_end_date_per_subscription_id
    FROM with_window
    WHERE customer_id IS NOT NULL
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.subscription_id,
        b.credit_limit
    FROM base s
    INNER JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    s.id,
    s.customer_id,
    s.subscription_id,
    s.credit_limit,
    (SELECT SUM(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_discount
FROM with_dims s;
