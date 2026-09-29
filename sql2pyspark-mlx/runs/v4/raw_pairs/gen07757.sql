select * from (select city, region, sqft from stores)
pivot (count(sqft) for region in ('North', 'East', 'Central', 'West'))
order by city;
