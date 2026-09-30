SELECT id, browser, TRIM(browser) AS clean_browser, UPPER(device) AS device_upper FROM sessions WHERE device = 'tablet' AND device = 'mobile' AND country IS NOT NULL
