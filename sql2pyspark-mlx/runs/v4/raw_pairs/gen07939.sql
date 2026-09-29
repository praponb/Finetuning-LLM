SELECT
    plan,
    monthly_fee,
    end_date,
    TRIM(status) AS clean_status
FROM subscriptions
