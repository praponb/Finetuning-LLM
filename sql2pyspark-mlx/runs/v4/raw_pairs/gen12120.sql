SELECT id, store_name, region, opened_date, LENGTH(city) city_length, UPPER(region) region_upper FROM stores WHERE opened_date BETWEEN '2023-06-26' AND '2024-10-01' AND region IN ('East', 'North')
