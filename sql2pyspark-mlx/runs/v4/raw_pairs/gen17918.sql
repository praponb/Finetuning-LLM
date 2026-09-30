WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND plan IN ('basic', 'enterprise')
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM cleaned e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.end_date >= sub.start_date
       AND e.end_date < sub.end_date + INTERVAL 7 DAYS
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.age,
        b.segment,
        b.country
    FROM with_window s
    INNER JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    monthly_fee_whole,
    subscription_id,
    CASE WHEN status RLIKE '^(CANCELLED|PAUSED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS status_zone
FROM joined
ORDER BY monthly_fee_whole;
