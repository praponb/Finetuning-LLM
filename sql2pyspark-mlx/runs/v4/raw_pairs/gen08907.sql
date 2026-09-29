SELECT opened_date, SUBSTRING(store_name, 1, 3) AS store_name_prefix, REGEXP_REPLACE(store_name, ' ', '') AS store_name_slug FROM stores WHERE region = 'Central' AND region = 'North';
