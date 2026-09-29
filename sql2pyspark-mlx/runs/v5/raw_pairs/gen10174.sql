SELECT id, city, region
FROM stores
WHERE opened_date <= '2023-08-28' AND region NOT IN ('East', 'Central', 'South');
