SELECT country, started_at, TRIM(country) clean_country, CASE WHEN device = 'tablet' THEN 1 ELSE 0 END is_tablet FROM sessions WHERE device IN ('desktop', 'tablet', 'mobile');
