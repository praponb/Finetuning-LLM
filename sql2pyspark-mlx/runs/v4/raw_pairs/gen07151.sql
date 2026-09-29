select id, device, started_at, browser, substring(browser, 1, 3) as browser_prefix from sessions where device not in ('mobile', 'desktop', 'tablet')
