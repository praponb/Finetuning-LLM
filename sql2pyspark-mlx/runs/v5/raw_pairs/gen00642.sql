SELECT *
FROM (
    SELECT YEAR(end_date) AS end_year, MIN(monthly_fee) AS min_monthly_fee, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
    FROM subscriptions
    GROUP BY YEAR(end_date)
) AS agg
WHERE min_monthly_fee < 170
