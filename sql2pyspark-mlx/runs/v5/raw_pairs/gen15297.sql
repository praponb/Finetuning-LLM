select
    dim.country,
    dim.device,
    count(*) as rows_total,
    round(sum(s.duration_sec), 2) as total_duration_sec
from web_events s
inner join sessions dim on s.session_id = dim.id
group by rollup (dim.country, dim.device);
