with top_customers as (
    select *, dense_rank() over (partition by city order by age desc) as pos
    from customers
)
select id, city, age
from top_customers
where pos <= 2
order by id;
