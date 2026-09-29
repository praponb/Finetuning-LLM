with ranked as (
    select *, row_number() over (partition by carrier order by cost desc, id) as row_num
    from shipments
    qualify row_num = 1
)
select
    date_trunc('year', shipped_date) as year_start,
    status,
    count(*) as cnt,
    percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    count_if(status = 'RETURNED') as returned_events,
    grouping_id() as gid
from ranked
group by rollup (date_trunc('year', shipped_date), status)
order by cnt desc;
