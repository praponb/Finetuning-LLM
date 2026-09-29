select id, device, started_at, country, coalesce(browser, 'N/A') as browser_filled from sessions where country <> 'TH';
