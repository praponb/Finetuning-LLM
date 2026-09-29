select * from (select city, region, sqft from stores)
pivot (sum(sqft) for region in ('East', 'West', 'North'))
order by city;
