SELECT id, store_name, TRIM(region) AS clean_region
FROM stores
WHERE opened_date > '2025-02-12' AND (store_name LIKE '%Downto%' OR region IN ('Central', 'West'));
