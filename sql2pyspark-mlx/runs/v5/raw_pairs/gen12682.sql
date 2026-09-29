select * from (select region, count(*) as num_stores from stores where sqft != 11376 group by region) as t where num_stores < 6 order by num_stores
