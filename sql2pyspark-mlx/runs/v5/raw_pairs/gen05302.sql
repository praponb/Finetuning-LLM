select id, browser, started_at, coalesce(country, 'unassigned') as country_filled from sessions;
