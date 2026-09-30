select *
from (
    select city, sum(age) total_age
    from customers
    group by city
) sub
where total_age > 260
order by city
