with numbered as (
    select
        id,
        customer_id,
        order_date,
        store_id,
        rank() over (partition by customer_id order by order_date) as row_num
    from orders
    where channel in ('web', 'mobile', 'partner')
)
select *
from numbered
where row_num <= 3
order by store_id desc, order_date;
