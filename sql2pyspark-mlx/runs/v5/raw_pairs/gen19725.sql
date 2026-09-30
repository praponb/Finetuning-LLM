with lines as (
    select
        id,
        service,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) e as k, value
),
unpacked as (
    select
        id,
        service,
        level,
        k,
        value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as has_fault
    from lines
    where k = 'trace'
),
metrics as (
    select
        id,
        service,
        level,
        k,
        value,
        has_fault,
        ntile(3) over (partition by service order by status_code_high desc, id, k) as status_code_high_bucket,
        sum(has_fault) over (partition by k order by has_fault, id, k rows between unbounded preceding and current row) as cumulative_has_fault,
        ntile(5) over (partition by k order by has_fault, id, k) as has_fault_bucket
    from unpacked
),
cleaned as (
    select
        id,
        level,
        k,
        value,
        has_fault,
        status_code_high_bucket,
        cumulative_has_fault,
        has_fault_bucket,
        substring(level, 1, 3) as level_prefix
    from metrics
    where has_fault_bucket = 7
)
select
    level,
    case when status_code_high_bucket >= 7 then 'LARGE' else 'SMALL' end as status_code_high_bucket_tier,
    sum(case when level = 'INFO' then has_fault_bucket else 0 end) as info_has_fault_bucket,
    count(distinct level) as distinct_levels,
    size(collect_set(k)) as num_ks,
    percent_rank() over (partition by level order by size(collect_set(k)) desc) as num_ks_percent_rank
from cleaned
group by level, case when status_code_high_bucket >= 7 then 'LARGE' else 'SMALL' end
order by status_code_high_bucket_tier desc;
