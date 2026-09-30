SELECT device FROM sessions WHERE country != 'DE' UNION ALL SELECT device FROM sessions WHERE country = 'TH';
