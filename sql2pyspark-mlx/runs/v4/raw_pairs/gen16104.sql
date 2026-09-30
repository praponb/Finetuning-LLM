WITH FilteredSubscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE customer_id IN (13, 19, 22, 33) AND (monthly_fee >= 190 OR status NOT IN ('EXPIRED', 'PAUSED', 'ACTIVE'))
),
agg AS (
    SELECT status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM FilteredSubscriptions
    GROUP BY status
)
SELECT *
FROM agg
WHERE stddev_monthly_fee < 63
ORDER BY stddev_monthly_fee DESC
