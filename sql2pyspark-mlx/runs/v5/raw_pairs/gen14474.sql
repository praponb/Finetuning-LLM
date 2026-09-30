with top_subscriptions as (
    select
        id,
        plan,
        end_date,
        dense_rank() over (partition by plan order by end_date desc) as rn
    from subscriptions
)
select id, plan, end_date
from top_subscriptions
where rn <= 3
order by id desc;
