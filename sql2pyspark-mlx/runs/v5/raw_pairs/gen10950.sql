select id, sqft, opened_date from stores where sqft = (select min(sqft) from stores);
