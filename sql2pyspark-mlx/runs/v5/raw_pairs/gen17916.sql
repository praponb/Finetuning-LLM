with ranked as (
    select *, row_number() over (partition by customer_id order by started_at desc, id) as dedup_rank
    from sessions
    qualify dedup_rank = 1
)
select
    customer_id,
    count(distinct device) as distinct_devices,
    count(case when device = 'mobile' then 1 end) as mobile_count,
    min(started_at) as first_started_at,
    min(started_at) as first_seen
from ranked
group by 1
order by customer_id desc;
