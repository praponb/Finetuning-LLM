select id, sqft from stores where sqft <= (select min(sqft) from stores) order by sqft;
