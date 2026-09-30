with lines as (
    select
        id,
        service,
        level,
        key,
        value
    from app_logs
    lateral view explode(context) e as key, value
)
select
    service,
    count(case when service = 'billing' then 1 end) as billing_count,
    count_if(level = 'ERROR') as error_events,
    count_if(level = 'ERROR') as error_events_2
from lines
group by service
order by error_events desc;
