SELECT id, city, CASE WHEN sqft >= 16267 THEN 'High' WHEN sqft >= 2972 THEN 'Medium' ELSE 'Low' END AS sqft_band FROM stores WHERE opened_date <= '2025-09-23' OR YEAR(opened_date) >= 2025;
