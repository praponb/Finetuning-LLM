select
    segment,
    max(credit_limit) - min(credit_limit) as credit_limit_range,
    count(case when city = 'Singapore' then 1 end) as singapore_count,
    count(*) as cnt
from customers
group by 1
having count(case when city = 'London' then 1 end) < 5
order by cnt desc;
