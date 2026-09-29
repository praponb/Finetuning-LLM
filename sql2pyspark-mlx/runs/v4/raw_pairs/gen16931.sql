SELECT *, CONCAT(browser, ', ', device) browser_device, DATE_TRUNC('WEEK', started_at) started_at_period FROM sessions WHERE started_at >= '2023-07-30'
