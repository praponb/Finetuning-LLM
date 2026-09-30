select * from (select region, city, sqft from stores where region in ('South', 'North')) pivot (count(sqft) for city in ('Singapore', 'Chiang Mai')) order by region;
