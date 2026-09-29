with numbered as (
    select *, row_number() over (partition by order_id order by amount desc, id) as row_num
    from payments
    where status = 'SUCCESS'
)
select *
from numbered
where row_num <= 3;
