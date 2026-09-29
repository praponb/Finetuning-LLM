select region, max(sqft) as max_sqft from stores where sqft between 6013 and 14923 group by 1;
