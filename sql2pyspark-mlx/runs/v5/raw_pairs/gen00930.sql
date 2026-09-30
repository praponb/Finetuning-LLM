with Ranked as (
    select *, row_number() over (partition by status order by shipping_fee desc, id) as pos
    from orders
)
select *
from Ranked
where pos <= 1
order by status;
