SELECT sqft, opened_date, city, INITCAP(TRIM(city)) city_title, LAST_DAY(opened_date) opened_date_month_end FROM stores WHERE LOWER(store_name) LIKE '%ive%' AND sqft <= 7861;
