select city, sum(case when region = 'East' then sqft else 0 end) as east_sqft from stores where sqft = 6082 and city = 'London' group by city;
