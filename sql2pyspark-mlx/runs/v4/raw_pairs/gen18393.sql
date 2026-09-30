with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) e as key, v
),
unpacked as (
    -- Semi-structured parsing
    select
        id,
        latency_ms,
        status_code,
        key,
        v,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as is_flagged
    from exploded
    where latency_ms between 608 and 2174
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(key)) as key_title,
        status_code + latency_ms as status_code_plus_latency_ms
    from unpacked
    where key is not null
      and status_code_high <= 2
)
-- Aggregate with conditional metrics
select
    case when status_code_plus_latency_ms >= 83 then 'HIGH' else 'LOW' end as status_code_plus_latency_ms_tier,
    case when status_code >= 418 then 'HIGH' else 'LOW' end as status_code_tier,
    sort_array(collect_set(key)) as key_list,
    count(distinct id) as distinct_ids,
    sort_array(collect_set(key)) as key_list_2
from filtered
group by case when status_code_plus_latency_ms >= 83 then 'HIGH' else 'LOW' end, case when status_code >= 418 then 'HIGH' else 'LOW' end
order by status_code_tier nulls first;
