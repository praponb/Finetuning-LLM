select region, sum(case when region = 'North' then 1 else 0 end) as north_count from stores where city = 'London' group by 1
