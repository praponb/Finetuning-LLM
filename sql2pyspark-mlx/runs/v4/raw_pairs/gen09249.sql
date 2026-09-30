select country, count(rating) as rating_count, count(*) as cnt
from suppliers
where city in ('Berlin', 'Chiang Mai')
group by country
having count(*) < 3
order by 2 desc, cnt;
