SELECT
    id,
    started_at,
    country,
    device,
    LOWER(browser) AS browser_lower,
    LENGTH(country) AS country_length
FROM sessions
WHERE device NOT IN ('mobile', 'desktop')
