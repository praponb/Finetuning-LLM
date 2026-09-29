SELECT customer_id, TRIM(browser) AS clean_browser FROM sessions WHERE browser IN ('Firefox', 'Chrome') AND (browser IN ('Firefox', 'Edge', 'Chrome') OR country IS NULL) ORDER BY customer_id DESC
