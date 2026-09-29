select region, count(case when region = 'North' then 1 end) north_count, count(distinct sqft) unique_regions
from stores
where city = 'Chiang Mai'
group by region
having count(distinct sqft) < 5
order by unique_regions desc
