with exploded as (
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) x as k, attr_value
),
unpacked as (
    select
        id,
        level,
        k,
        attr_value,
        cast((status_code & 8) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
metrics as (
    select
        *,
        row_number() over w_running as event_seq,
        ntile(4) over w_grp as has_fault_bucket
    from unpacked
    window w_running as (order by status_code_high asc, id asc, k asc), w_grp as (partition by level, k order by has_fault desc, id asc, k asc)
)
select
    case when event_seq >= 9 then 'LARGE' else 'SMALL' end as event_seq_tier,
    k,
    max(has_fault) filter (where level = 'INFO') as info_max_has_fault,
    min(has_fault_bucket) as min_has_fault_bucket,
    sum(case when level = 'DEBUG' then has_fault else 0 end) as debug_has_fault
from metrics
group by grouping sets ((case when event_seq >= 9 then 'LARGE' else 'SMALL' end, k), (case when event_seq >= 9 then 'LARGE' else 'SMALL' end))
having sum(case when level = 'DEBUG' then has_fault else 0 end) >= 0;
