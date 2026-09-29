SELECT
    id,
    status,
    CASE WHEN monthly_fee >= 233 THEN 'High' WHEN monthly_fee >= 54 THEN 'Medium' ELSE 'Low' END AS monthly_fee_level,
    DATE_SUB(start_date, 18) AS start_date_minus_18d
FROM subscriptions
ORDER BY id DESC, monthly_fee_level;
