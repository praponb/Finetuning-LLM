SELECT
    plan,
    monthly_fee_bucket,
    monthly_fee_count,
    monthly_fee_range
FROM (
    SELECT
        plan,
        CASE WHEN monthly_fee >= 247 THEN 'high' WHEN monthly_fee >= 118 THEN 'mid' ELSE 'low' END AS monthly_fee_bucket,
        COUNT(monthly_fee) AS monthly_fee_count,
        MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY plan, CASE WHEN monthly_fee >= 247 THEN 'high' WHEN monthly_fee >= 118 THEN 'mid' ELSE 'low' END
) AS sub
WHERE monthly_fee_count < 8;
