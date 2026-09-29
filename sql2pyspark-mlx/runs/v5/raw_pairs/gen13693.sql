select city, sum(case when city = 'New York' then 1 else 0 end) as new_york_count
from suppliers
group by city
order by 2 desc, city
limit 5;
