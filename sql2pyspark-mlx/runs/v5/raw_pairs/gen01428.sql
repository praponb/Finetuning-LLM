SELECT
    id,
    country,
    browser,
    customer_id,
    DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions
WHERE device <> 'desktop'
