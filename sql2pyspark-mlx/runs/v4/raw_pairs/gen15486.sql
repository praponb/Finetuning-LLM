SELECT
    id,
    country,
    customer_id,
    browser,
    CASE WHEN device IN ('desktop', 'tablet') THEN 'Tier 1' ELSE 'Other' END AS device_group
FROM sessions
WHERE HOUR(started_at) BETWEEN 11 AND 19
ORDER BY browser DESC, id
LIMIT 20
