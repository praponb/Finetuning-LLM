SELECT *, CONCAT(country, ', ', browser) AS country_browser, DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label FROM sessions WHERE country NOT IN ('TH', 'JP') AND country = 'TH';
