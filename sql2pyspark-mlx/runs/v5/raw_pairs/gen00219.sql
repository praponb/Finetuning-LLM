SELECT
    id,
    start_date,
    plan,
    end_date,
    CONCAT_WS('-', plan, status) AS plan_status_key,
    WEEKOFYEAR(start_date) AS start_date_weekofyear
FROM subscriptions
WHERE monthly_fee >= 110
ORDER BY id DESC, end_date
LIMIT 5
