with ranked as (
    select
        id,
        channel,
        amount,
        dense_rank() over (partition by channel order by amount desc) as rnk
    from orders
)
select *
from ranked
where rnk <= 3
order by amount desc
