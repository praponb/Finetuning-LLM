SELECT
    id,
    monthly_fee,
    status,
    COALESCE(monthly_fee, 0) AS monthly_fee_filled
FROM subscriptions
WHERE status NOT IN ('CANCELLED', 'PAUSED') AND YEAR(end_date) >= 2024
ORDER BY id DESC, 4;
