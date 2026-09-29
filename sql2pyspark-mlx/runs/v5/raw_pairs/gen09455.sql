SELECT plan, expired_count
FROM (
    SELECT plan, SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count
    FROM subscriptions
    GROUP BY plan
) AS s
WHERE expired_count < 56
