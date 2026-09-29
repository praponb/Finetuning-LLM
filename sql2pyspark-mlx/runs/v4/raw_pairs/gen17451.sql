with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        service,
        host,
        level,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        coalesce(context['trace'], 'none') as trace_or_default,
        context['region'] as region,
        cast((status_code & 1) != 0 as int) as is_flagged
    from app_logs
)
-- Aggregate with conditional metrics
select
    case when is_flagged >= 1 then 'HIGH' else 'LOW' end as is_flagged_tier,
    count(*) as cnt,
    size(collect_set(service)) as num_services,
    size(collect_set(service)) as num_services_2,
    count(case when service = 'billing' then 1 end) as billing_count,
    grouping_id() as grouping_level
from unpacked
group by rollup (case when is_flagged >= 1 then 'HIGH' else 'LOW' end)
order by billing_count nulls first
