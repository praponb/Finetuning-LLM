SELECT
    id,
    start_date,
    plan,
    end_date,
    SUBSTRING(plan, 1, 4) plan_prefix,
    LAST_DAY(end_date) end_date_month_end
FROM subscriptions;
