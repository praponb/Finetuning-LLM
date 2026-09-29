with latest as (
    select *, row_number() over (partition by device order by started_at nulls first, id) as dedup_rank
    from sessions
    qualify dedup_rank = 1
)
select
    date_trunc('quarter', started_at) as quarter_start,
    count(*) as cnt,
    max(started_at) as last_started_at,
    size(collect_set(country)) as num_countries
from latest
group by date_trunc('quarter', started_at)
having cnt > 1
order by quarter_start desc nulls first;
