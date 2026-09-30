select
    location,
    sum(budget) total_budget,
    count(*) cnt,
    rank() over (order by sum(budget) desc) as rnk
from departments
where budget >= 3241000
group by location
order by cnt;
