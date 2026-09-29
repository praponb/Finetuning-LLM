select id, sqft from stores where sqft > (select max(sqft) from stores);
