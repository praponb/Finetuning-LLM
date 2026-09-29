select * from (select city, region, sqft from stores) pivot (sum(sqft) for region in ('North', 'Central', 'West', 'East')) order by city;
