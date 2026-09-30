select * from (select city, region, sqft from stores) src pivot (sum(sqft) as total, count(1) as n for region in ('North', 'South', 'East'));
