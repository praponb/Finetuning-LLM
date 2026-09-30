with Numbered as (
    select *, rank() over (partition by store_id order by revenue desc) as rn
    from store_sales
)
select id, store_id, revenue
from Numbered
where rn <= 5
order by id desc
