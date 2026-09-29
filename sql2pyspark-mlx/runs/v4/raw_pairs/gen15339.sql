SELECT *, INITCAP(TRIM(region)) AS region_title, CONCAT_WS('-', city, region) AS city_region_key FROM stores WHERE region <> 'Central' OR store_name LIKE '%Tow%'
