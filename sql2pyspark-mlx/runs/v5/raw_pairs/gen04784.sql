SELECT
    id,
    plan,
    monthly_fee,
    DATE_SUB(start_date, 68) AS start_date_minus_68d,
    CASE WHEN monthly_fee >= 230 THEN 'High' WHEN monthly_fee >= 66 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
FROM subscriptions
WHERE monthly_fee <= 180
ORDER BY start_date_minus_68d, id DESC;
