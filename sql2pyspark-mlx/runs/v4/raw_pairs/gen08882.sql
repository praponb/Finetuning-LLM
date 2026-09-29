SELECT sqft, opened_date, CASE WHEN sqft >= 13231 THEN 'High' WHEN sqft >= 7870 THEN 'Medium' ELSE 'Low' END AS sqft_level
FROM stores
ORDER BY sqft_level DESC, sqft;
