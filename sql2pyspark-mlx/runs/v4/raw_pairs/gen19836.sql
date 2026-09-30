SELECT id, city, store_name, ADD_MONTHS(opened_date, 12) AS opened_date_plus_months FROM stores WHERE sqft <> 4656 AND sqft > 14314 ORDER BY opened_date_plus_months DESC;
