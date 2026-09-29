SELECT
    customer_id,
    country,
    browser,
    LOWER(browser) browser_lower
FROM sessions
WHERE browser IN ('Edge', 'Chrome', 'Firefox')
ORDER BY browser, country DESC;
