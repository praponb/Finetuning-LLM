select id, sqft, opened_date, substring(store_name, 1, 3) as store_name_prefix from stores where sqft <= 6401;
