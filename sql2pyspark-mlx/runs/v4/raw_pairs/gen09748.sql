with top_web_events as (
    select *, rank() over (partition by page order by duration_sec desc) as rnk
    from web_events
)
select *
from top_web_events
where rnk <= 3
