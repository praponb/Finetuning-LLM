with Ranked as (
    select *, row_number() over (partition by status order by discount desc, id) as row_num
    from orders
)
select id, status, discount
from Ranked
where row_num <= 1;
