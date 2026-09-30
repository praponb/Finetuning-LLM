select city, min_sqft from (select city, min(sqft) as min_sqft from stores group by city) as sub where min_sqft >= 15500
