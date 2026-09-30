SELECT
    id,
    customer_id,
    end_date,
    plan,
    INITCAP(TRIM(plan)) plan_title
FROM subscriptions;
