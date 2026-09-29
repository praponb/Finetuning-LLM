select id, sqft, opened_date from stores where sqft != 15535 union select id, sqft, opened_date from stores where store_name like '%Old%'
