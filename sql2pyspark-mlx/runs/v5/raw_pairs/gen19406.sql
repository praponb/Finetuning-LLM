SELECT
    id,
    started_at,
    customer_id,
    country,
    CASE WHEN country IN ('JP', 'DE') THEN 'Group A' ELSE 'Other' END AS country_group
FROM sessions
WHERE TO_DATE(started_at) >= '2024-03-04'
ORDER BY country DESC;
