SELECT
    id,
    status,
    end_date,
    CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'Priority' ELSE 'Other' END AS status_group
FROM subscriptions
WHERE MONTH(end_date) IN (2, 10, 11);
