SELECT id, started_at, COALESCE(browser, 'N/A') AS browser_filled FROM sessions;
