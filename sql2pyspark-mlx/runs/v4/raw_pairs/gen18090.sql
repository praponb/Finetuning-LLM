SELECT
    id,
    end_date,
    status,
    CAST(monthly_fee AS INT) AS monthly_fee_int,
    DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
FROM subscriptions
WHERE plan IN ('standard', 'premium')
