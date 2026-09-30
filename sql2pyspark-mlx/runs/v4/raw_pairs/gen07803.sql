select * from (select region, city, sqft from stores) src pivot (sum(sqft) as total for city in ('Berlin' as berlin, 'Chiang Mai' as chiang_mai, 'London' as london));
