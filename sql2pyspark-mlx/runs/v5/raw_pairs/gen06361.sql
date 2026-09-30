with ranked as (
    select *, rank() over (partition by order_id order by amount desc) as rnk
    from payments
)
select *
from ranked
where rnk <= 2
order by amount desc
