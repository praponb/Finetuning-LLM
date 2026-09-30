with flattened as (
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        v
    from app_logs
    lateral view explode(context) li as k, v
),
parsed as (
    select
        id,
        level,
        status_code,
        k,
        cast((status_code & 1) != 0 as int) as has_fault,
        shiftright(status_code, 2) as status_code_high
    from flattened
    where k != 'region'
),
ranked as (
    select *, rank() over (partition by k order by has_fault desc) as latest_rank
    from parsed
    qualify latest_rank = 1
)
select
    case when status_code >= 393 then 'PREMIUM' else 'STANDARD' end as status_code_tier,
    bit_or(status_code) as status_code_bit_or,
    count(case when level = 'ERROR' then 1 end) as error_count,
    size(collect_set(level)) as num_levels,
    sum(case when level = 'INFO' then status_code else 0 end) as info_status_code
from ranked
group by case when status_code >= 393 then 'PREMIUM' else 'STANDARD' end
order by info_status_code;
