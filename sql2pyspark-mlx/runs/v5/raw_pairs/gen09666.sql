select city, sum(case when region = 'North' then sqft else 0 end) as north_sqft, avg(sqft) as avg_sqft from stores where opened_date > '2024-07-11' group by city;
