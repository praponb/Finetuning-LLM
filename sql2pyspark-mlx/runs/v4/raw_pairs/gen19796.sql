with src as (
    select *
    from orders
    where discount is not null
),
summary as (
    select case when channel in ('store', 'mobile') then 'priority' else 'other' end as channel_group, round((100 * sum(case when status = 'PENDING' then 1 else 0 end)) / count(*), 1) as pct_pending, sum(case when channel = 'web' then 1 else 0 end) as web_count
    from src
    group by case when channel in ('store', 'mobile') then 'priority' else 'other' end
)
select channel_group, pct_pending
from summary
order by pct_pending desc;
