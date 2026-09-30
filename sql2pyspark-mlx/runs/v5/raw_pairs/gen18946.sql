with numbered as (
    select *, row_number() over (partition by country order by age, id) pos
    from customers
)
select id, country, age
from numbered
where pos <= 1
order by age desc;
